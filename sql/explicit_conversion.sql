CREATE TABLE public.t1 (
    id smallint PRIMARY KEY,
    name varchar(10),
    date varchar(10)
);

INSERT INTO public.t1 (id, name, date)
    VALUES (1, 'foo', '0000-00-00');

COMMENT ON COLUMN public.t1.date IS $$REPLACE(%I, '0000-00-00', '1970-01-01')::date$$;

-- columns are disordered voluntarily
CREATE TABLE public.t2 (
    id smallint PRIMARY KEY,
    date date,
    name varchar(10)
);

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
  ('public.t1', 'public.t2', null, 1, 1, true, null, null);

-- plan(1) should use the REPLACE function on `date` column
SELECT invocation FROM plan() \gexec

-- table public.t2 should have 1 row with date '1970-01-01'
SELECT * FROM public.t2;
