\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS tools;
SET search_path = tools;

-- "state" enum represents the state of a job
DO $$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'state') THEN
    CREATE TYPE state AS ENUM ('running', 'failed', 'pending', 'completed');
END IF;
END$$;

-- "config" table represents the configuration of relation
CREATE TABLE IF NOT EXISTS config (
    config_id bigint generated always as identity primary key,
    source regclass not null,
    target regclass not null,
    pkey text not null,
    priority numeric not null default 1,
    condition text,
    batchsize integer,
    trunc boolean not null default true
);

-- "stage" table represents a execution of several jobs
CREATE TABLE IF NOT EXISTS stage (
    stage_id bigint generated always as identity primary key,
    ts timestamp default now()
);

-- "job" table represents the state of table copies for each stage
CREATE TABLE IF NOT EXISTS job (
    stage_id bigint not null references stage(stage_id) on delete cascade,
    job_id bigint generated always as identity,
    config_id bigint not null references config(config_id) on delete cascade,
    lastseq bigint not null default 0,
    rows bigint not null default 0,
    elapsed interval,
    ts timestamp,
    state state not null default 'pending',
    PRIMARY KEY (stage_id, job_id)
);

-- "copy" procedure transfers data from source to target
-- using the configuration in the "job" table
CREATE OR REPLACE PROCEDURE tools.copy(p_id bigint)
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

    -- Truncate the target relation if option is set
    -- and the job has not been started yet
    IF r.trunc AND r.ts IS NULL THEN
        stmt := format('TRUNCATE %s', r.target);
        raise notice 'Executing: %', stmt;
        EXECUTE stmt;
    END IF;

    -- Update job record on start
    UPDATE tools.job
        SET state = 'running', ts = COALESCE(ts, clock_timestamp())
        WHERE job_id = r.job_id;
    COMMIT;

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
        v_start := clock_timestamp();
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

        -- Update job record at the end of each batch
        v_elapsed := clock_timestamp() - v_start;
        r.elapsed := COALESCE(r.elapsed, '0') + v_elapsed;
        r.rows := r.rows + v_rows;

        UPDATE tools.job
            SET lastseq = r.lastseq, rows = r.rows, elapsed = r.elapsed
            WHERE job_id = r.job_id;
        COMMIT;

        EXIT WHEN r.batchsize IS NULL;
    END LOOP;

    -- Update job record on completion
    UPDATE tools.job 
        SET state = r.state 
        WHERE job_id = r.job_id;
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
    SELECT format('SELECT %s FROM %s', string_agg(format('%I', attname), ', '), p_source)
      FROM pg_attribute
     WHERE attrelid = p_target
       AND attnum > 0 AND NOT attisdropped
     GROUP BY p_target;
$$;

-- "plan" function inserts a new stage record and returns the statements to execute
-- "targets" parameter is used to filter the target relations
CREATE OR REPLACE FUNCTION tools.plan(targets text[] DEFAULT '{}'::text[])
RETURNS TABLE (statement text, target regclass)
LANGUAGE SQL AS $$
    WITH targets AS (
        SELECT s::regclass AS target
        FROM unnest(targets) s
        UNION
        SELECT DISTINCT target FROM tools.config
        WHERE cardinality(targets) = 0
    ), new_stage AS (
        INSERT INTO tools.stage DEFAULT VALUES
        RETURNING stage_id
    ), new_jobs AS (
        INSERT INTO tools.job (stage_id, config_id, lastseq)
            -- Get the last sequence number from the last stage
            -- if target may not be truncated. Otherwise, use 0
            SELECT stage_id, config_id, COALESCE(
                (SELECT max(lastseq) FROM tools.job
                   JOIN tools.config USING (config_id)
                  WHERE config_id = c.config_id AND NOT trunc), 0)
            FROM tools.config c
            JOIN targets USING (target)
            CROSS JOIN new_stage
        RETURNING job_id, config_id
    )
    SELECT format('CALL tools.copy(%s);', job_id), target
      FROM new_jobs
      JOIN tools.config USING (config_id)
      ORDER BY priority, job_id;
$$;

-- "report" view returns the state of the last stage for each relation
-- with a special column "rate" that shows the number of rows per second
CREATE OR REPLACE VIEW tools.report AS
SELECT r.stage_id, c.target, min(j.ts) job_start, min(j.state) state,
       sum(j.rows) rows, max(j.elapsed) elapsed,
       sum(round(j.rows / extract(epoch from j.elapsed), 2)) AS rate
  FROM tools.job j
  JOIN tools.config c USING (config_id)
  JOIN tools.stage r USING (stage_id)
 GROUP BY r.stage_id, c.target;
