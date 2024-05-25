\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS fdw;

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
    parts integer not null default 1,
    trunc boolean not null default true,
    condition text,
    batchsize integer,
    PRIMARY KEY (source, target)
);

-- "stage" table represents a execution of several jobs
CREATE TABLE IF NOT EXISTS stage (
    stage_id bigint generated always as identity primary key,
    ts timestamp default now()
);

-- "job" table represents the state of table copies for each stage
CREATE TABLE IF NOT EXISTS job (
    job_id bigint generated always as identity,
    stage_id bigint not null references stage(stage_id) on delete cascade,
    config_id bigint not null references config(config_id) on delete cascade,
    target regclass not null,
    lastseq bigint not null default 0,
    rows bigint not null default 0,
    elapsed interval,
    ts timestamp,
    state state not null default 'pending',
    PRIMARY KEY (job_id)
);

CREATE INDEX ON job(target);

-- "task" table represents the configuration of each job
CREATE TABLE IF NOT EXISTS task (
    job_id bigint not null references job(job_id),
    source regclass not null,
    target regclass not null,
    pkey text not null,
    trunc boolean not null default true
    condition text,
    batchsize integer,
);

CREATE INDEX ON task(job_id);

-- "copy" procedure transfers data from source to target
-- using the configuration in the "job" table
CREATE OR REPLACE PROCEDURE copy(p_id bigint)
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
        FROM fdw.job JOIN fdw.task USING (job_id)
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
    UPDATE fdw.job
        SET state = 'running', ts = COALESCE(ts, clock_timestamp())
        WHERE job_id = r.job_id;
    COMMIT;

    LOOP
        -- Build INSERT statement
        SELECT statement INTO stmt FROM fdw.columns(r.target, r.source);
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

        UPDATE fdw.job
            SET lastseq = r.lastseq, rows = r.rows, elapsed = r.elapsed
            WHERE job_id = r.job_id;
        COMMIT;

        EXIT WHEN r.batchsize IS NULL;
    END LOOP;

    -- Update job record on completion
    UPDATE fdw.job 
        SET state = r.state 
        WHERE job_id = r.job_id;
    COMMIT;

    IF r.state = 'failed' THEN
        RAISE E'%\nCONTEXT:  %', v_message, v_ctx;
    END IF;
END;
$$;

-- "columns" function returns SELECT statement for the source relation
-- with the column names of the target relation
CREATE OR REPLACE FUNCTION columns(p_target regclass, p_source regclass)
RETURNS TABLE (statement text)
LANGUAGE SQL AS $$
    SELECT format('SELECT %s FROM %s', string_agg(format('%I', attname), ', '), p_source)
      FROM pg_attribute
     WHERE attrelid = p_target
       AND attnum > 0 AND NOT attisdropped
     GROUP BY p_target;
$$;

-- "lastseq" function return the upper sequence number from the previous stages
CREATE OR REPLACE FUNCTION lastseq(p_config_id regclass)
RETURNS bigint LANGUAGE sql AS
$$ SELECT COALESCE(MAX(lastseq), 0) FROM job WHERE config_id = p_config_id $$;

-- "plan" function prepares a new stage by creating new job and task records
-- "targets" parameter is used to filter the target relations
CREATE OR REPLACE FUNCTION plan(targets text[] DEFAULT '{}'::text[])
RETURNS TABLE (target regclass, invocation text)
LANGUAGE SQL AS $$
    WITH configs AS (
        SELECT c.* AS target FROM fdw.config c
        JOIN unnest(targets) s ON c.target = s::regclass
        UNION
        SELECT * FROM fdw.config
        WHERE cardinality(targets) = 0
        ORDER BY priority, condition
    ), new_stage AS (
        INSERT INTO fdw.stage DEFAULT VALUES
        RETURNING stage_id
    ), new_jobs AS (
        INSERT INTO fdw.job (stage_id, config_id, target, lastseq)
            SELECT stage_id, config_id, target,
                   CASE WHEN NOT trunc THEN lastseq(config_id) ELSE 0 END
            FROM new_stage CROSS JOIN configs
        RETURNING job_id, config_id, target
    ), new_tasks AS (
        INSERT INTO fdw.task(job_id, source, target, pkey, condition, batchsize, trunc)
            SELECT job_id, source, c.target, pkey, condition, batchsize, trunc
            FROM new_jobs j
            JOIN fdw.config c USING (config_id)
            RETURNING job_id, target
    )
    SELECT target, format('CALL fdw.copy(%s);', job_id)
      FROM new_tasks;
$$;

-- "report" view returns the state of the last stage for each relation
-- with a special column "rate" that shows the number of rows per second
CREATE OR REPLACE VIEW report AS
SELECT s.stage_id, j.target, min(j.ts) job_start, min(j.state) state,
       sum(j.rows) rows, max(j.elapsed) elapsed,
       sum(round(j.rows / extract(epoch from j.elapsed), 2)) AS rate
  FROM job j
  JOIN stage s USING (stage_id)
 GROUP BY s.stage_id, j.target;
