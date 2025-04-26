CREATE SCHEMA source;

-- "parent" and "child" are involved in a foreign key
CREATE TABLE source.parent (id int);
CREATE TABLE public.parent (id int primary key);

CREATE TABLE source.child (id int, parent_id int);
CREATE TABLE public.child (id int primary key, parent_id int not null references public.parent(id));

INSERT INTO config (source, target, pkey, trunc) VALUES
-- a truncate should failed if target is involved in a foreign key
    ('source.parent', 'public.parent', 'id', true);

SELECT invocation FROM plan('{public.parent}');

-- copy(1) should fail because "parent" is referenced by "child"
CALL copy(1);

-- job 1 should have a failed state
SELECT stage_id, target, rows, state
  FROM report WHERE stage_id = 1 ORDER BY target;
