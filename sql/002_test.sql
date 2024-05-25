
UPDATE source.t1 SET name = null WHERE id = 1;
INSERT INTO source.t2 (id, age, name) SELECT 2002, 2^(16-1), 'foo';

SELECT target, statement FROM plan();

-- copy(4) should fail because of the NOT NULL constraint
CALL copy(4);

-- copy(5) should fail because of out of range value
CALL copy(5);

-- copy(7) should fail because job_id does not exist
CALL copy(7);

-- copy(6) should succeed
CALL copy(6);

SELECT stage_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE stage_id = 2;

SELECT stage_id, target, rows, state
  FROM report WHERE stage_id = 2 ORDER BY target;
