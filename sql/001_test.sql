
INSERT INTO tools.config (source, target, pkey, condition, batchsize, trunc) VALUES
-- t1 will be copied in a single operation
    ('source.t1', 'public.t1', 'id', null, null, true),
-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
    ('source.t2', 'public.t2', 'id', 'id % 2 = 0', 200, false),
    ('source.t2', 'public.t2', 'id', 'id % 2 = 1', 200, false);

SELECT * FROM config;

SELECT target, statement FROM run();

CALL start(1);

SELECT run_id, job_id, config_id, lastseq, rows FROM job;

CALL start(2);
CALL start(3);

SELECT run_id, job_id, config_id, lastseq, rows FROM job;

INSERT INTO source.t2 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1001, 2000) i;

CALL start(2);
CALL start(3);

SELECT run_id, target, rows FROM report ORDER BY target;
