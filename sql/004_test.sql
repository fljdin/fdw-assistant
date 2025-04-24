-- withkeywords table is used to test reserved keywords
-- in "extract" method
CREATE TABLE withkeywords1 (
    id serial primary key,
    "limit" integer not null
);

INSERT INTO withkeywords1 (id, "limit") VALUES (1, 1);

CREATE TABLE withkeywords2 (
    id serial primary key,
    "limit" integer not null
);

INSERT INTO config (source, target, pkey, condition, batchsize, trunc) VALUES
    ('withkeywords1', 'withkeywords2', 'id', null, null, true);

SELECT target, invocation FROM plan('{withkeywords2}');

-- copy(1) should succeed and "limit" must be quoted
CALL copy(1);
