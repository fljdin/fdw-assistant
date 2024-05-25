
SET tools.targets = 'public.t1';

-- Should return a single job for public.t1
SELECT target, statement FROM run();

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 3;

RESET tools.targets;

-- Should return all jobs
SELECT target, statement FROM run();

SELECT run_id, job_id, config_id, lastseq, rows, state
  FROM job WHERE run_id = 4;
