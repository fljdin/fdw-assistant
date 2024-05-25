
-- Should return a single job for public.t1
SELECT target, statement FROM plan('{public.t1}');

SELECT stage_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE stage_id = 3;

-- Should return all jobs
SELECT target, statement FROM plan();

SELECT stage_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE stage_id = 4;

-- Should fail as the table does not exist
SELECT target, statement FROM plan('{public.foo}');

-- Should return an empty set as the table has no configuration
SELECT target, statement FROM plan('{public.dummy}');
