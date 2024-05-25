\set ON_ERROR_STOP on

INSERT INTO tools.config (relname, source, pkey, condition, batchsize, trunc) VALUES
    ('public.t1', 'source.t1', 'id', null, null, true),
    ('public.t2', 'source.t2', 'id', 'id % 2 = 0', 200, false),
    ('public.t2', 'source.t2', 'id', 'id % 2 = 1', 200, false);
