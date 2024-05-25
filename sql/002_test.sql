
UPDATE source.t1 SET name = null WHERE id = 1;
INSERT INTO source.t2 (id, age, name) SELECT 2002, 2^(16-1), 'foo';

SELECT target, statement FROM run();

-- start(4) should fail because of the NOT NULL constraint
CALL start(4);

-- start(5) should fail because of out of range value
CALL start(5);

-- start(7) should fail because job_id does not exist
CALL start(7);

-- start(6) should succeed
CALL start(6);

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 2;

SELECT run_id, target, rows, state
  FROM report WHERE run_id = 2 ORDER BY target;
