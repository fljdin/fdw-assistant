\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS tools;
SET search_path = tools;

-- "config" table represents the configuration of relation
CREATE TABLE IF NOT EXISTS config (
    config_id bigint generated always as identity primary key,
    relname regclass not null,
    source regclass not null,
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
    run_id bigint not null references run(run_id),
    job_id bigint generated always as identity,
    config_id bigint not null references config(config_id),
    lastseq bigint not null default 0,
    rows bigint not null default 0,
    elapsed interval not null default '0'::interval,
    PRIMARY KEY (run_id, job_id)
);

-- "start" procedure copies data from source to target
-- using the configuration in the "job" table
CREATE OR REPLACE PROCEDURE tools.start(p_id bigint)
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    v_start timestamp;
    v_elapsed interval;
    v_rows bigint;
    stmt text;
BEGIN
    SELECT * INTO r
        FROM tools.job
        JOIN tools.config USING (config_id)
        WHERE job_id = p_id;
    IF r.trunc THEN
        stmt := format('TRUNCATE %s', r.relname);
        r.lastseq := 0;
        r.elapsed := '0';
        r.rows := 0;

        raise notice 'Executing: %', stmt;
        EXECUTE stmt;
    END IF;

    LOOP
        -- Build INSERT statement
        SELECT statement INTO stmt FROM tools.extract(r.relname, r.source);
        stmt := format('INSERT INTO %1$I %2$s WHERE %3$I > %4$s %5$s ORDER BY %3$I %6$s',
            r.relname, stmt, r.pkey, r.lastseq,
            CASE WHEN r.condition IS NOT NULL THEN format('AND %s', r.condition) ELSE '' END,
            CASE WHEN r.batchsize IS NOT NULL THEN format('LIMIT %s', r.batchsize) ELSE '' END
        );
        raise notice 'Executing: %', stmt;
        stmt := format('WITH inserted AS (%1$s RETURNING %2$s) SELECT max(%2$s) AS lastseq, count(*) AS rows FROM inserted',
            stmt, r.pkey
        );

        -- Execute INSERT statement
        v_start := clock_timestamp();
        EXECUTE stmt INTO r.lastseq, v_rows;
        EXIT WHEN v_rows = 0;

        -- Update job record
        v_elapsed := clock_timestamp() - v_start;
        r.elapsed := r.elapsed + v_elapsed;
        r.rows := r.rows + v_rows;

        UPDATE tools.job
           SET lastseq = r.lastseq, rows = r.rows, elapsed = r.elapsed
         WHERE job_id = r.job_id;

        COMMIT;
        EXIT WHEN r.batchsize IS NULL;
    END LOOP;
END;
$$;

-- "extract" function returns SELECT statement for the source relation
-- with the column names of the target relation
CREATE OR REPLACE FUNCTION tools.extract(p_relname regclass, p_source regclass)
RETURNS TABLE (statement text)
LANGUAGE SQL AS $$
    SELECT format('SELECT %s FROM %s', string_agg(format('%I', attname), ', '), p_source)
      FROM pg_attribute
     WHERE attrelid = p_relname
       AND attnum > 0 AND NOT attisdropped
     GROUP BY p_relname;
$$;

-- "newrun" function truncate relations, inserts a new run record,
-- and returns the statements to execute
CREATE OR REPLACE FUNCTION tools.newrun()
RETURNS TABLE (statement text, relname regclass)
LANGUAGE SQL AS $$
    WITH new_run AS (
        INSERT INTO tools.run DEFAULT VALUES
        RETURNING run_id
    ), new_job AS (
        INSERT INTO tools.job (run_id, config_id)
            SELECT run_id, config_id
            FROM tools.config
            CROSS JOIN new_run
        RETURNING job_id, config_id
    )
    SELECT format('CALL tools.start(%s);', job_id), relname
      FROM new_job
      JOIN tools.config USING (config_id);
$$;

-- "run" function returns statements attached to an existing run
CREATE OR REPLACE FUNCTION tools.run(p_run_id bigint)
RETURNS TABLE (statement text, relname regclass)
LANGUAGE SQL AS $$
    SELECT format('CALL tools.start(%s);', job_id), relname
      FROM tools.job
      JOIN tools.config USING (config_id)
     WHERE run_id = p_run_id;
$$;

-- "report" view returns the state of the last run for each relation
-- with a special column "rate" that shows the number of rows per second
CREATE OR REPLACE VIEW tools.report AS
SELECT r.run_id, c.relname, min(r.ts) run_start, sum(j.rows) rows, max(j.elapsed) elapsed,
       sum(round(j.rows / extract(epoch from j.elapsed), 2)) AS rate
  FROM tools.job j
  JOIN tools.config c USING (config_id)
  JOIN tools.run r USING (run_id)
 WHERE elapsed > '0'::interval
 GROUP BY r.run_id, c.relname;
