-- withkeywords table is used to test reserved keywords
-- in "extract" method
CREATE TABLE "WithKeywords1" (
    id serial primary key,
    "limit" integer not null
);

INSERT INTO "WithKeywords1" (id, "limit") VALUES (1, 1);

CREATE TABLE "WithKeywords2" (
    id serial primary key,
    "limit" integer not null
);

INSERT INTO config (source, target, pkey, condition, batchsize, trunc) VALUES
    ('"WithKeywords1"', '"WithKeywords2"', 'id', null, null, true);

SELECT target, invocation FROM plan('{\"WithKeywords2\"}');

-- copy(1) should succeed and "limit" must be quoted
CALL copy(1);
