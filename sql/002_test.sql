INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
    ('source.t1', 'public.t1', 'id', 100, 1, true, null, null),
    ('source.t2', 'public.t2', 'id', 1, 1, false, null, null);

UPDATE source.t1 SET name = null WHERE id = 1;
INSERT INTO source.t2 (id, age, name) SELECT 1001, 2^(16-1), 'foo';

SELECT target, invocation FROM plan('{public.t1, public.t2}');

-- copy(1) should fail because of out of range value
CALL copy(1);

-- copy(2) should fail because of the NOT NULL constraint
CALL copy(2);

SELECT stage_id, job_id, target, part, lastseq, rows, state
  FROM job WHERE stage_id = 1 ORDER BY job_id;

SELECT stage_id, target, rows, state
  FROM report WHERE stage_id = 1 ORDER BY target;
