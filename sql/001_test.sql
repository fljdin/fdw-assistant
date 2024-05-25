
INSERT INTO tools.config (source, target, pkey, priority, condition, batchsize, trunc) VALUES
-- t1 will be copied in a single operation
    ('source.t1', 'public.t1', 'id', 100, null, null, true),
-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
    ('source.t2', 'public.t2', 'id', 1, 'id % 2 = 0', 200, false),
    ('source.t2', 'public.t2', 'id', 1, 'id % 2 = 1', 200, false);

SELECT * FROM config;

-- "run" must order the jobs by priority (lower first)
SELECT target, statement FROM run();

-- "report" view shows the aggregated state for each target table
SELECT * FROM report WHERE run_id = 1;

-- start(1) should truncate public.t1 as configured
CALL start(1);

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 1 ORDER BY job_id;

-- start(2) and start(3) should share an half of the data from source.t2
CALL start(2);
CALL start(3);

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 1 ORDER BY job_id;

INSERT INTO source.t2 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1001, 2000) i;

-- start(2) and start(3) should continue from where they left
-- whithout truncating the target table
CALL start(2);
CALL start(3);

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 1 ORDER BY job_id;

SELECT run_id, target, rows, state
  FROM report WHERE run_id = 1 ORDER BY target;
