
UPDATE source.t1 SET name = null WHERE id = 1;
INSERT INTO source.t2 (id, age, name) SELECT 2002, 2^(16-1), 'foo';

SELECT target, invocation FROM plan('{public.t1, public.t2}');

-- copy(4) should fail because of out of range value
CALL copy(4);

-- copy(5) should succeed
CALL copy(5);

-- copy(6) should fail because of the NOT NULL constraint
CALL copy(6);

-- copy(100) should fail because job_id does not exist
CALL copy(100);

SELECT stage_id, job_id, target, part, lastseq, rows, state
  FROM job WHERE stage_id = 2 ORDER BY job_id;

SELECT stage_id, target, rows, state
  FROM report WHERE stage_id = 2 ORDER BY target;
