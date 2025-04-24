INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
-- t3 will be copied with a condition that filters negative values
    ('source.t3', 'public.t3', 'id', 200, 2, true, 'value >= 0', null);

SELECT * FROM config WHERE target = 'public.t3'::regclass;

SELECT invocation FROM plan('{public.t3}');

-- copy(10) and copy(11) should copy the positive values from source.t3
-- only job with part #0 should truncate the target table
CALL copy(1);
CALL copy(2);

-- public.t3 should have 100 rows
SELECT count(*) FROM public.t3;
