CREATE TABLE public.dummy1 ();
CREATE TABLE public.dummy2 ();

INSERT INTO config (source, target, condition) VALUES
  ('public.dummy1', 'public.dummy2', 'must fail');

-- copy(1) should fail with a syntax error
SELECT invocation FROM plan() \gexec

-- target public.dummy2 should have a failed state
SELECT target, state, rows, total FROM report WHERE stage_id = 1 ORDER BY job_start;
