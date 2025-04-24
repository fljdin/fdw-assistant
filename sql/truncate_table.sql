CREATE SCHEMA source;

CREATE TABLE source.t1 (
    id serial primary key,
    name text
);

INSERT INTO source.t1 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1, 100) i;

-- name has a NOT NULL constraint to test edge cases
CREATE TABLE public.t1 (
    id serial primary key,
    name text not null
);

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
-- t1 will be copied in a single operation
    ('source.t1', 'public.t1', 'id', 100, 1, true, null, null);
    
SELECT target, invocation FROM plan();

-- copy(1) should truncate public.t1 as it is a new stage
CALL copy(1);

-- copy(1) should do nothing more because there is no new data in source.t1
CALL copy(1);

SELECT stage_id, job_id, target, part, lastseq, rows, total, state
  FROM job WHERE stage_id = 1 ORDER BY job_id;