CREATE SCHEMA source;
CREATE TABLE source.t1 ();
CREATE TABLE public.t1 ();
CREATE TABLE source.t2 ();
CREATE TABLE public.t2 ();

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
    ('source.t1', 'public.t1', 'id', 1, 1, true, null, null),
    ('source.t2', 'public.t2', 'id', 2, 2, false, null, null);

-- the first stage should plan 3 jobs
SELECT target, invocation FROM plan('{public.t1, public.t2}');

-- updating "config" table do not affect the previous jobs
UPDATE config SET parts = 1;

-- the next stage should plan one single job per target
SELECT target, invocation FROM plan('{public.t1, public.t2}');

-- stage #1 had 3 jobs, stage #1 gets 2 jobs
SELECT stage_id, count(*) jobs, count(distinct target) targets
FROM job WHERE stage_id IN (1, 2)
GROUP BY stage_id;
