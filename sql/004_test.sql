
INSERT INTO config (source, target, pkey, condition, batchsize, trunc) VALUES
    ('source.withkeywords', 'public.withkeywords', 'id', null, null, true);

SELECT target, statement FROM plan('{public.withkeywords}');

-- start(11) should succeed and "limit" must be quoted
CALL start(11);
