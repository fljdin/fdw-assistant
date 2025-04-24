-- public.t1 contains negative values that we wont copy
CREATE TABLE public.t1 (
    id serial primary key,
    value smallint not null
);

INSERT INTO public.t1 (id, value)
    SELECT i, i - 100 FROM generate_series(1, 199) i;

CREATE TABLE public.t2 (
    id serial primary key,
    value smallint not null CHECK (value >= 0)
);

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
-- t1 will be copied with a condition that filters negative values
    ('public.t1', 'public.t2', 'id', 200, 2, true, 'value >= 0', null);

SELECT * FROM config WHERE target = 'public.t2'::regclass;

SELECT invocation FROM plan('{public.t2}');

-- copy(1) and copy(2) should copy the positive values from source.t1
-- only job with part #0 should truncate the target table
CALL copy(1);
CALL copy(2);

-- public.t2 should have 100 rows
SELECT count(*) FROM public.t2;
