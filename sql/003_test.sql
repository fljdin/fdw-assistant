-- Should fail as the table does not exist
SELECT target, invocation FROM plan('{public.foo}');

-- Should return an empty set as the table has no configuration
SELECT target, invocation FROM plan('{public.dummy}');

-- copy(100) should fail because job_id does not exist
CALL copy(100);
