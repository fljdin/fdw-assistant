
INSERT INTO config (source, target, pkey, trunc) VALUES
-- a truncate should failed if target is involved in a foreign key
    ('source.parent', 'public.parent', 'id', true);

SELECT invocation FROM plan('{public.parent}');

-- copy(12) should fail because "parent" is referenced by "child"
CALL copy(12);
