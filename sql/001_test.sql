
INSERT INTO config (source, target, pkey, priority, condition, batchsize, trunc) VALUES
-- t1 will be copied in a single operation
    ('source.t1', 'public.t1', 'id', 100, null, null, true),
-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
    ('source.t2', 'public.t2', 'id', 1, 'id % 2 = 0', 200, false),
    ('source.t2', 'public.t2', 'id', 1, 'id % 2 = 1', 200, false);

SELECT * FROM config;

-- "plan" must order the jobs by priority (lower first)
SELECT target, statement FROM plan();

-- "report" view shows the aggregated state for each target table
SELECT * FROM report WHERE stage_id = 1;

-- copy(1) should truncate public.t1 as it is a new stage
CALL copy(1);

-- copy(1) should do nothing more because there is no new data in source.t1
CALL copy(1);

SELECT stage_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE stage_id = 1 ORDER BY job_id;

-- copy(2) and copy(3) should share an half of the data from source.t2
CALL copy(2);
CALL copy(3);

SELECT stage_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE stage_id = 1 ORDER BY job_id;

INSERT INTO source.t2 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1001, 2000) i;

-- copy(2) and copy(3) should continue from where they left
CALL copy(2);
CALL copy(3);

SELECT stage_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE stage_id = 1 ORDER BY job_id;

SELECT stage_id, target, rows, state
  FROM report WHERE stage_id = 1 ORDER BY target;
