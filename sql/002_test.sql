
UPDATE source.t1 SET name = null WHERE id = 1;
UPDATE source.t2 SET age = 2^(16-1) WHERE id = 1802;
TRUNCATE TABLE public.t2;

SELECT target, statement FROM run();

-- start(4) should fail because of the NOT NULL constraint
CALL start(4);

-- start(5) should fail because of out of range value
CALL start(5);

-- start(6) should succeed
CALL start(6);

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 2;

SELECT run_id, target, rows, state
  FROM report WHERE run_id = 2 ORDER BY target;
