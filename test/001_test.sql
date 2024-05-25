\i test/psql.sql

INSERT INTO source.t1 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1, 100) i;

INSERT INTO source.t2 (id, name)
    SELECT i, 'name' || i FROM generate_series(1, 1000) i;

INSERT INTO tools.config (relname, source, pkey, condition, batchsize, trunc) VALUES
-- t1 will be copied in a single operation
    ('public.t1', 'source.t1', 'id', null, null, true),
-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
    ('public.t2', 'source.t2', 'id', 'id % 2 = 0', 200, false),
    ('public.t2', 'source.t2', 'id', 'id % 2 = 1', 200, false);

SELECT plan(5);

SELECT results_eq(
    'SELECT statement FROM tools.run()',
    ARRAY[
        'CALL tools.start(1);',
        'CALL tools.start(2);',
        'CALL tools.start(3);'
    ],
    'Launch a run of two tables'
);

CALL tools.start(1);

SELECT set_eq(
    $$ SELECT rows FROM tools.job WHERE job_id = 1 $$,
    ARRAY[100],
    'The first table has 100 rows'
);

CALL tools.start(2);
CALL tools.start(3);

SELECT set_eq(
    $$ SELECT rows FROM tools.job WHERE job_id = 2 $$,
    ARRAY[500],
    'The job copied 500 rows'
);

SELECT set_eq(
    $$ SELECT sum(rows) FROM tools.job 
         JOIN tools.config USING (config_id) 
        WHERE relname = 't2'::regclass $$,
    ARRAY[1000],
    'Both jobs copied 1000 rows'
);

-- Insert more 1000 rows in t2
INSERT INTO source.t2 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1001, 2000) i;

CALL tools.start(2);
CALL tools.start(3);

SELECT set_eq(
    $$ SELECT sum(rows) FROM tools.job 
         JOIN tools.config USING (config_id) 
        WHERE relname = 't2'::regclass $$,
    ARRAY[2000],
    'Both jobs copied 2000 rows'
);

SELECT * FROM finish();
