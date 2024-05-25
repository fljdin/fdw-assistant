
INSERT INTO config (source, target, pkey, condition, batchsize, trunc) VALUES
    ('source.withkeywords', 'public.withkeywords', 'id', null, null, true);

SELECT target, invocation FROM plan('{public.withkeywords}');

-- copy(17) should succeed and "limit" must be quoted
CALL copy(17);
