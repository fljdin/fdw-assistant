
-- updating "config" table do not affect the previous jobs
UPDATE config SET parts = 1;

-- we want to plan the next stage with one single job per target copy
SELECT target, invocation FROM plan('{public.t1, public.t2, public.t3}');

-- stage #1 had 5 jobs, stage #8 gets 3 jobs
SELECT stage_id, count(*) jobs, count(distinct target) targets
FROM job WHERE stage_id IN (1, 8)
GROUP BY stage_id;
