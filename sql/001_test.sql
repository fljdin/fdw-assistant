
INSERT INTO tools.config (relname, source, pkey, condition, batchsize, trunc) VALUES
-- t1 will be copied in a single operation
    ('public.t1', 'source.t1', 'id', null, null, true),
-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
    ('public.t2', 'source.t2', 'id', 'id % 2 = 0', 200, false),
    ('public.t2', 'source.t2', 'id', 'id % 2 = 1', 200, false);

SELECT * FROM config;

SELECT relname, statement FROM run();

CALL start(1);

SELECT run_id, job_id, config_id, lastseq, rows FROM job;

CALL start(2);
CALL start(3);

SELECT run_id, job_id, config_id, lastseq, rows FROM job;

INSERT INTO source.t2 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1001, 2000) i;

CALL start(2);
CALL start(3);

SELECT run_id, relname, rows FROM report ORDER BY relname;
