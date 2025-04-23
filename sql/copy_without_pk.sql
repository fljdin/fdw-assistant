CREATE TABLE public.t1 (
    id smallint primary key
);

INSERT INTO public.t1 (id)
    SELECT i FROM generate_series(1, 100) i;

CREATE TABLE public.t2 (
    id smallint primary key
);

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
-- t1 will be copied in a single batch
    ('public.t1', 'public.t2', null, 1, 1, true, null, null);

SELECT * FROM config WHERE target = 'public.t2'::regclass;

-- copy(1) should be a simple SELECT with no condition
SELECT invocation FROM plan() \gexec

-- Add a condition
UPDATE config SET condition = 'id >= 0' WHERE target = 'public.t2'::regclass;

-- copy(2) should be a SELECT with a condition
SELECT invocation FROM plan() \gexec
