CREATE SCHEMA source;

-- source.t1 is large enough to test copy in batches and through several jobs
CREATE TABLE source.t1 (
    id serial primary key,
    age int not null,
    name text not null
);

INSERT INTO source.t1 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1, 1000) i;

-- columns order differs from source.t1 and public.t1
CREATE TABLE public.t1 (
    id serial primary key,
    name text not null,
    age smallint not null
);

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
-- t1 will be dispatched to two jobs, each will insert data with a batch size of 200
    ('source.t1', 'public.t1', 'id', 1, 2, false, null, 200);

SELECT target, invocation FROM plan();

-- "report" view shows the aggregated state for each target table
SELECT target, state, rows FROM report WHERE stage_id = 1;

-- copy(1) and copy(2) should share an half of the data from source.t1
CALL copy(1);
CALL copy(2);

SELECT stage_id, job_id, target, part, lastseq, rows, total, state
  FROM job WHERE stage_id = 1 ORDER BY job_id;

INSERT INTO source.t1 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1001, 2000) i;

-- copy(1) and copy(2) should continue from where they left
CALL copy(1);
CALL copy(2);

-- Each job must have a non-zero elapsed time
SELECT stage_id, job_id, target, part, lastseq, rows, total, state
  FROM job WHERE stage_id = 1 AND elapsed > interval '0s' ORDER BY job_id;

-- "report" view should compiles the state of the last stage for each relation
SELECT stage_id, target, rows, total, state
  FROM report WHERE stage_id = 1 ORDER BY target;
