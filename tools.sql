\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS tools;
SET search_path = tools;

-- "tools.targets" setting is used to filter the source relations
ALTER DATABASE :DBNAME SET tools.targets TO '';

-- "state" enum represents the state of a job
CREATE TYPE state AS ENUM ('pending', 'running', 'failed', 'completed');

-- "config" table represents the configuration of relation
CREATE TABLE IF NOT EXISTS config (
    config_id bigint generated always as identity primary key,
    source regclass not null,
    target regclass not null,
    pkey text not null,
    condition text,
    batchsize integer,
    trunc boolean not null default true
);

-- "run" table represents each from-scratch execution
CREATE TABLE IF NOT EXISTS run (
    run_id bigint generated always as identity primary key,
    ts timestamp default now()
);

-- "job" table represents the state of table copies for each run
CREATE TABLE IF NOT EXISTS job (
    run_id bigint not null references run(run_id) on delete cascade,
    job_id bigint generated always as identity,
    config_id bigint not null references config(config_id) on delete cascade,
    lastseq bigint not null default 0,
    rows bigint not null default 0,
    elapsed interval,
    ts timestamp,
    state state not null default 'pending',
    PRIMARY KEY (run_id, job_id)
);

-- "start" procedure copies data from source to target
-- using the configuration in the "job" table
CREATE OR REPLACE PROCEDURE tools.start(p_id bigint)
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    stmt text;
    v_ctx text;
    v_elapsed interval;
    v_message text;
    v_rows bigint;
    v_start timestamp;
BEGIN
    SELECT * INTO r
        FROM tools.job JOIN tools.config USING (config_id)
        WHERE job_id = p_id;

    -- Raise an exception if the job does not exist
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', p_id;
    END IF;

    IF r.trunc THEN
        stmt := format('TRUNCATE %s', r.target);
        r.lastseq := 0;
        r.elapsed := '0';
        r.rows := 0;

        raise notice 'Executing: %', stmt;
        EXECUTE stmt;
    END IF;

    v_start := clock_timestamp();
    UPDATE tools.job 
        SET state = 'running', ts = COALESCE(ts, v_start)
        WHERE job_id = r.job_id;

    LOOP
        -- Build INSERT statement
        SELECT statement INTO stmt FROM tools.extract(r.target, r.source);
        stmt := format('INSERT INTO %1$s %2$s WHERE %3$s > %4$s %5$s ORDER BY %3$s %6$s',
            r.target, stmt, r.pkey, r.lastseq,
            CASE WHEN r.condition IS NOT NULL THEN format('AND %s', r.condition) ELSE '' END,
            CASE WHEN r.batchsize IS NOT NULL THEN format('LIMIT %s', r.batchsize) ELSE '' END
        );
        raise notice 'Executing: %', stmt;
        stmt := format('WITH inserted AS (%1$s RETURNING %2$s) SELECT max(%2$s) AS lastseq, count(*) AS rows FROM inserted',
            stmt, r.pkey
        );

        -- Execute INSERT statement
        BEGIN
            EXECUTE stmt INTO r.lastseq, v_rows;
            r.state := 'completed';
        EXCEPTION WHEN OTHERS THEN
            r.state := 'failed';
            GET STACKED DIAGNOSTICS
                v_ctx     = PG_EXCEPTION_CONTEXT,
                v_message = MESSAGE_TEXT;
            EXIT;
        END;
        EXIT WHEN v_rows = 0;

        -- Update job record
        v_elapsed := clock_timestamp() - v_start;
        r.elapsed := r.elapsed + v_elapsed;
        r.rows := r.rows + v_rows;

        UPDATE tools.job
            SET lastseq = r.lastseq, rows = r.rows, elapsed = r.elapsed
            WHERE job_id = r.job_id;

        EXIT WHEN r.batchsize IS NULL;
    END LOOP;

    UPDATE tools.job SET state = r.state WHERE job_id = r.job_id;
    COMMIT;

    IF r.state = 'failed' THEN
        RAISE E'%\nCONTEXT:  %', v_message, v_ctx;
    END IF;
END;
$$;

-- "extract" function returns SELECT statement for the source relation
-- with the column names of the target relation
CREATE OR REPLACE FUNCTION tools.extract(p_target regclass, p_source regclass)
RETURNS TABLE (statement text)
LANGUAGE SQL AS $$
    SELECT format('SELECT %s FROM %s', string_agg(format('%s', attname), ', '), p_source)
      FROM pg_attribute
     WHERE attrelid = p_target
       AND attnum > 0 AND NOT attisdropped
     GROUP BY p_target;
$$;

-- "run" function inserts a new run record and returns the statements to execute
-- "tools.targets" setting is used to filter the target relations
CREATE OR REPLACE FUNCTION tools.run()
RETURNS TABLE (statement text, target regclass)
LANGUAGE SQL AS $$
    WITH new_run AS (
        INSERT INTO tools.run DEFAULT VALUES
        RETURNING run_id
    ), targets AS (
        SELECT s::regclass AS target
        FROM string_to_table(current_setting('tools.targets'),',') AS t(s)
        UNION
        SELECT DISTINCT target FROM tools.config
        WHERE current_setting('tools.targets') = ''
    ), new_job AS (
        INSERT INTO tools.job (run_id, config_id)
            SELECT run_id, config_id
            FROM tools.config
            JOIN targets USING (target)
            CROSS JOIN new_run
        RETURNING job_id, config_id
    )
    SELECT format('CALL tools.start(%s);', job_id), target
      FROM new_job
      JOIN tools.config USING (config_id);
$$;

-- "report" view returns the state of the last run for each relation
-- with a special column "rate" that shows the number of rows per second
CREATE OR REPLACE VIEW tools.report AS
SELECT r.run_id, c.target, min(j.ts) job_start, min(j.state) state,
       sum(j.rows) rows, max(j.elapsed) elapsed,
       sum(round(j.rows / extract(epoch from j.elapsed), 2)) AS rate
  FROM tools.job j
  JOIN tools.config c USING (config_id)
  JOIN tools.run r USING (run_id)
 GROUP BY r.run_id, c.target;
