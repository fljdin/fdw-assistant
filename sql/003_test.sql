
-- Should return a single job for public.t1
SELECT target, statement FROM run('{public.t1}');

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 3;

-- Should return all jobs
SELECT target, statement FROM run();

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 4;

-- Should fail as the table does not exist
SELECT target, statement FROM run('{public.foo}');

-- Should return an empty set as the table has no configuration
SELECT target, statement FROM run('{public.dummy}');
