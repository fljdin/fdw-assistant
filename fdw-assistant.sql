\set ON_ERROR_STOP on
\if :{?INSTALL}
\else
\set INSTALL 'assistant'
\endif

CREATE SCHEMA IF NOT EXISTS :INSTALL;
ALTER DATABASE :DBNAME SET assistant.search_path TO :INSTALL;
SET search_path = :INSTALL;

-- "state" enum represents the state of a job
DO $$
BEGIN
IF NOT EXISTS (
    SELECT typname, nspname FROM pg_type JOIN pg_namespace n ON typnamespace = n.oid
    WHERE typname = 'state' and nspname = current_setting('search_path')
) THEN
    CREATE TYPE state AS ENUM ('running', 'failed', 'pending', 'completed');
END IF;
END $$;

-- "config" table represents the main configuration
CREATE TABLE IF NOT EXISTS config (
    source regclass not null,
    target regclass not null,
    pkey text,
    priority numeric not null default 1,
    parts integer not null default 1,
    trunc boolean not null default true,
    condition text,
    batchsize integer,
    PRIMARY KEY (source, target)
);

-- "stage" table represents an execution of several jobs
CREATE TABLE IF NOT EXISTS stage (
    stage_id bigint generated always as identity primary key,
    ts timestamp default now()
);

-- "job" table represents the state of table copies for each stage
CREATE TABLE IF NOT EXISTS job (
    job_id bigint generated always as identity,
    stage_id bigint not null references stage(stage_id) on delete cascade,
    target regclass not null,
    part integer not null default 0,
    lastseq bigint not null default 0,
    rows bigint not null default 0,
    total bigint,
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
    pkey text,
    trunc boolean not null default true,
    condition text,
    batchsize integer
);

CREATE UNIQUE INDEX ON task(job_id);

-- "report" view returns the state of the last stage for each relation
-- with a special column "rate" that shows the number of rows per second
CREATE OR REPLACE VIEW report AS
SELECT r.*,
       CASE WHEN total IS NULL OR total = 0 THEN null
            ELSE trunc(rows / total, 2) END AS progress,
       CASE WHEN total IS NULL OR rate = 0 THEN null
            ELSE format('%s hours', trunc(total / rate / 60 / 60, 2))::interval
            END AS eti,
       CASE WHEN total IS NULL OR rate = 0 THEN null
            ELSE job_start + format('%s hours', trunc(total / rate / 60 / 60, 2))::interval
            END AS eta
  FROM (
    SELECT s.stage_id, j.target, min(j.ts) job_start, min(j.state) state,
        sum(j.rows) rows, sum(j.total) total, max(j.elapsed) elapsed,
        sum(round(j.rows / extract(epoch from j.elapsed), 2)) AS rate
    FROM job j
    JOIN stage s USING (stage_id)
    GROUP BY s.stage_id, j.target
  ) AS r;

-- "columns" function returns SELECT statement for the source relation
-- with the column names of the target relation
CREATE OR REPLACE FUNCTION columns(p_target regclass, p_source regclass)
RETURNS TABLE (statement text)
LANGUAGE SQL AS $$
    SELECT format('SELECT %s FROM %s',
            string_agg(
              format(coalesce(pg_catalog.col_description(p_source::oid, fa.attnum), '%I'), a.attname),
              ', ' ORDER BY a.attnum
            ), p_source)
      FROM pg_attribute a
      JOIN pg_attribute fa ON fa.attname = a.attname
     WHERE a.attrelid = p_target
       AND fa.attrelid = p_source
       AND a.attnum > 0 AND NOT a.attisdropped
     GROUP BY p_target;
$$;

-- "lastseq" function return the upper sequence number from the previous stages
CREATE OR REPLACE FUNCTION lastseq(p_target regclass, p_part integer)
RETURNS bigint SET search_path = :INSTALL LANGUAGE sql AS
$$ SELECT COALESCE(MAX(lastseq), 0) FROM job
    WHERE target = p_target AND part = p_part $$;

-- "plan" function prepares a new stage by creating new job and task records
-- "targets" parameter is used to filter the target relations
CREATE OR REPLACE FUNCTION plan(targets text[] DEFAULT '{}'::text[])
RETURNS TABLE (target regclass, invocation text)
SET search_path = :INSTALL LANGUAGE SQL AS $$
    WITH configs AS (
        SELECT c.* AS target FROM config c
        RIGHT JOIN unnest(targets) s ON c.target = s::regclass
        UNION
        SELECT * FROM config
        WHERE cardinality(targets) = 0
        ORDER BY priority, condition
    ), new_stage AS (
        INSERT INTO stage DEFAULT VALUES
        RETURNING stage_id
    ), new_jobs AS (
        INSERT INTO job (stage_id, target, part, lastseq)
            SELECT stage_id, target, part,
                   CASE WHEN NOT trunc THEN lastseq(target, part) ELSE 0 END
            FROM new_stage CROSS JOIN (
                SELECT * FROM configs
                CROSS JOIN LATERAL generate_series(0, parts - 1) AS part
            ) c
        RETURNING job_id, target, part
    ), new_tasks AS (
        INSERT INTO task(job_id, source, target, pkey, batchsize, trunc, condition)
            SELECT job_id, source, c.target, pkey, batchsize, trunc,
                CASE WHEN c.parts = 1 THEN condition
                     ELSE format('%s %% %s = %s%s', pkey, parts, part,
                        CASE WHEN condition IS NOT NULL
                             THEN format(' AND %s', condition)
                             ELSE '' END) END
            FROM new_jobs j
            JOIN config c USING (target)
            RETURNING job_id, target
    )
    SELECT target, format('CALL %s.copy(%s);', current_setting('search_path'), job_id)
      FROM new_tasks;
$$;

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
    v_total bigint;
    v_start timestamp;
    v_schema text;
    v_conditions text[];

BEGIN
    -- As we make transaction control, we could not use a global SET
    -- > If a SET clause is attached to a procedure, then that procedure cannot
    -- > execute transaction control statements Source
    -- https://www.postgresql.org/docs/current/sql-createprocedure.html
    v_schema := current_setting('assistant.search_path');
    EXECUTE format('SET search_path TO %I', v_schema);

    SELECT * INTO r
        FROM job JOIN task USING (job_id)
        WHERE job_id = p_id;

    -- Raise an exception if the job does not exist
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', p_id;
    END IF;

    -- Truncate the target relation if option is set
    -- only one job whould truncate the target table (part = 0)
    IF r.trunc AND r.ts IS NULL AND r.part = 0 THEN
        stmt := format('TRUNCATE %s', r.target);
        raise notice 'Executing: %', stmt;
        BEGIN
            EXECUTE stmt;
        EXCEPTION WHEN OTHERS THEN
            r.state := 'failed';
            GET STACKED DIAGNOSTICS
                v_ctx     = PG_EXCEPTION_CONTEXT,
                v_message = MESSAGE_TEXT;
            raise exception E'%\nCONTEXT:  %', v_message, v_ctx;
        END;
    END IF;

    -- Update job record on start
    UPDATE job
        SET state = 'running', ts = COALESCE(ts, clock_timestamp())
        WHERE job_id = r.job_id;
    COMMIT;

    IF r.pkey IS NOT NULL THEN
      v_conditions := array_append(v_conditions, format('%s > %s', r.pkey, r.lastseq));
    END IF;

    IF r.condition IS NOT NULL THEN
      v_conditions := array_append(v_conditions, format('%s', r.condition));
    END IF;

    -- Retrieve the total number of rows to be copied
    stmt := format('SELECT count(%2$s) FROM %1$s %3$s',
        r.source, COALESCE(r.pkey, '1'),
        CASE WHEN cardinality(v_conditions) > 0 THEN 'WHERE ' || array_to_string(v_conditions, ' AND ') ELSE '' END
    );
    raise notice 'Executing: %', stmt;

    v_start := clock_timestamp();
    EXECUTE stmt INTO v_total;
    v_elapsed := clock_timestamp() - v_start;
    r.elapsed := COALESCE(r.elapsed, '0') + v_elapsed;

    UPDATE job
        SET total = COALESCE(r.total, 0) + v_total, elapsed = r.elapsed
        WHERE job_id = r.job_id;
    COMMIT;

    LOOP
        -- Exit if there are no rows to copy
        EXIT WHEN r.total = 0;

        -- Build INSERT statement
        SELECT statement INTO stmt FROM columns(r.target, r.source);
        stmt := format('INSERT INTO %1$s %2$s %3$s %4$s %5$s',
            r.target, stmt,
            CASE WHEN cardinality(v_conditions) > 0 THEN 'WHERE ' || array_to_string(v_conditions, ' AND ') ELSE '' END,
            CASE WHEN r.pkey IS NOT NULL THEN format('ORDER BY %s', r.pkey) ELSE '' END,
            CASE WHEN r.batchsize IS NOT NULL THEN format('LIMIT %s', r.batchsize) ELSE '' END
        );
        raise notice 'Executing: %', stmt;

        -- Execute INSERT statement
        v_start := clock_timestamp();
        BEGIN
            IF r.pkey IS NOT NULL THEN
              stmt := format('WITH inserted AS (%1$s RETURNING %2$s) SELECT max(%2$s) AS lastseq, count(*) AS rows FROM inserted',
                  stmt, r.pkey
              );
              EXECUTE stmt INTO r.lastseq, v_rows;

            ELSE
              stmt := format('WITH inserted AS (%s RETURNING 1) SELECT count(*) AS rows FROM inserted', stmt);
              EXECUTE stmt INTO v_rows;
            END IF;

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

        UPDATE job
            SET lastseq = r.lastseq, rows = r.rows, elapsed = r.elapsed
            WHERE job_id = r.job_id;
        COMMIT;

        EXIT WHEN r.batchsize IS NULL;
    END LOOP;

    -- Update job record on completion
    UPDATE job
        SET state = r.state
        WHERE job_id = r.job_id;
    COMMIT;

    IF r.state = 'failed' THEN
        raise exception E'%\nCONTEXT:  %', v_message, v_ctx;
    END IF;
END;
$$;
