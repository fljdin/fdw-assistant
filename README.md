# tools

Orchestrate data migration from source tables to target tables with a simple
configuration table. The relations must exist and should be defined with the
same columns in no particular order.

Tools provide:

- a convenient way to build new **runs** based on configuration, composed by
  jobs attached to every configured source-to-table.
- a **report** view that aggregate **job** status per **run**
- a variety of options in the central **config** table

Tools have been designed to be used by a multiple processus orchestrator, as
`xargs` or [`dispatch`][dispatch] commands. In that way, a source table could be
processed in parallel by defining a modulus condition to split data in distinct
subsets.

[dispatch]: https://github.com/fljdin/dispatch

## Configuration

**config**

* `config_id` (type `integer`): A unique identifier attached to a job.

* `source` (type `regclass`): Where the data comes from, relative to current
  `search_path`.

* `target` (type `regclass`): Where the data goes to, relative to current
  `search_path`.

* `pkey` (type `text`): Column name included in primary key constraint.
  Composite columns are not supported.

* `priority` (type `numeric`): Sort the job list in ascendent order for a new
  **run**.

* `condition` (type `text`): Applies a `WHERE` condition to the `SELECT`
  statement used during data transfer. Usefull for split data in distinct
  subsets with a modulus condition.

* `batchsize` (type `interger`): If set, the job will loop over source target to
  transfer data as a bunch of rows with intermediate `COMMIT` at batch
  completion.

* `trunc` (type `boolean`): If set to `true`, the target table will be truncated
  before start a **job** of a new **run**.

Examples:

```sql
-- t1 will be copied in a single operation and must be truncated at new run
INSERT INTO tools.config
  (source, target, pkey, priority, condition, batchsize, trunc)
VALUES
  ('source.t1', 'public.t1', 'id', 100, null, null, true);

-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
INSERT INTO tools.config
  (source, target, pkey, priority, condition, batchsize, trunc)
VALUES
  ('source.t2', 'public.t2', 'id', 1, 'id % 2 = 0', 200, false),
  ('source.t2', 'public.t2', 'id', 1, 'id % 2 = 1', 200, false);
```

## API Usage

**run(targets text[])** function

* `run()` prepares a new run by creating `run` and `job` records and returns a
  set of `CALL start()` statements in order of configured priority.

* `targets` parameter is an array of target relation names used as filter. If
  empty, all relations in the `config` table are used as targets.

* A table with `trunc` option will be truncated at the beginning of a new run.

**start(job_id bigint)** procedure

* `start()` handles `INSERT` statement build and execution, updates its own job
  record during bulk insertion batches and at the end with `success` or `failed`
  status.

* The same `start(job_id)` statement can be executed several times without
  truncating a table with `trunc` option. This behavior is intended to resume
  the job from the last known sequence in case of unintentional interruption.

## Internal relations

**state** enum type

* `running`: a job is processing the data transfer

* `failed`: a error has been raised, the job stopped in the middle of a batch
  and rollbacked his current task.

* `pending`: a job has been prepared by the `run()` procedure but the `start()`
  has not been called yet.

* `completed`: a job has processed the data transfert successfully.

**report** view

* `run_id` (type `bigint`): Identifier used to filter a specific run.

* `target` (type `regclass`): Where the data goes to, relative to current
  `search_path`.

* `job_start` (type `timestamp`): Start time of the jobs, `null` if not started
  yet.

* `state` (type `state`): State of the jobs, `pending` by default.

* `rows` (type `numeric`): Number of rows processed by the jobs so far.

* `elapsed` (type `interval`): Cumulative elapsed time spent by the jobs.

* `rate` (type `numeric`): Calculated rate (rows per second) attached to a jobs,
  based on elapsed time and rows processed.

**run** table

* `run_id` (type `bigint`): A unique identifier spawned by calling `run()`
  function.

* `ts` (type `timestamp`): Creation time of the run.

**job** table

* `run_id` (type `bigint`): Identifier of the run that job belongs to.

* `job_id` (type `bigint`): A unique identifier to manipulate the job with
  `start()` procedure.

* `config_id` (type `bigint`): Identifier of the configuration attached to the
  job.

* `lastseq` (type `bigint`): Last maximum value returned by the `INSERT`
  statement, based on the `pkey`, to resume the data transfer with new rows.

* `rows` (type `bigint`): Cumulative number of rows processed.

* `elapsed` (type `interval`): Cumulative elapsed time spent by the job.

* `ts` (type `timestamp`): Start time of the job, `null` if not started yet.

* `state` (type `state`): State of the job, `pending` by default.

## Hack

**Generate oracle_fdw key options for each foreign table**

Source : https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns

```sql
SELECT format('ALTER FOREIGN TABLE fdw.%I ALTER COLUMN %I OPTIONS (ADD key ''true'')', c.relname, a.attname)
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
INNER JOIN pg_index i ON c.oid = i.indrelid
INNER JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary
WHERE n.nspname = 'public';
```

**Feed the config table from foreign tables definition**

```sql
INSERT INTO config (target, source, pkey, batchsize)
SELECT format('public.%I', c.relname), format('%I.%I', n.nspname, c.relname), a.attname, 100000
FROM pg_catalog.pg_foreign_table ft
INNER JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid
INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
INNER JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid,
LATERAL pg_catalog.pg_options_to_table(a.attfdwoptions) op
WHERE a.attnum > 0 AND NOT a.attisdropped
AND op.option_name = 'key' AND op.option_value = 'true';
```

**Split the export for multiple processes**

```sql
INSERT INTO config (source, target, pkey, condition, batchsize, trunc)
SELECT source, target, pkey, format('%s %% 4 = %s', pkey, i), 200000, false
  FROM config CROSS JOIN generate_series(0,3) i
 WHERE target = 'public.table1'::regclass AND condition IS NULL;

DELETE FROM config WHERE target = 'public.table1'::regclass AND condition IS NULL;
```
