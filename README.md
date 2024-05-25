# Foreign Data Wrapper Assistant

Orchestrate data transfer from foreign tables to local tables with a simple
configuration table. The relations must exist and should be defined with the
same columns in no particular order.

`fdw-assistant` provides:

- a convenient way to build new **stages** based on configuration, composed by
  preconfigured jobs
- a **report** view that aggregate **job** status per **stage**
- a variety of options in the central **config** table

`fdw-assistant` has been designed to be used by a multiple processus
orchestrator, like `xargs` or [`dispatch`][dispatch] commands. In that way, data
and subsets could be processed in parallel.

[dispatch]: https://github.com/fljdin/dispatch

## Installation

Download the main file and execute it on a PostgreSQL database.
It creates a dedicated schema, named `fdw` with relations and routines.

```sh
psql -f fdw-assistant.sql
```

To remove the assistant, just drop the schema.

```sql
DROP SCHEMA fdw CASCADE;
```

## Configuration

**config**

* `source` (type `regclass`): Where the data comes from, relative to current
  `search_path`.

* `target` (type `regclass`): Where the data goes to, relative to current
  `search_path`.

* `pkey` (type `text`): Column name included in primary key constraint.
  Composite columns are not supported.

* `priority` (type `numeric`): Used during **stage** creation to sort the job
  list in ascendent order.

* `parts` (type `bigint`) : Defines the number of subsets we want. Used during
  **stage** creation to build a modulus condition for each subset.

* `trunc` (type `boolean`): If set to `true`, the target table will be truncated
  at the very first start of a **job**.

* `condition` (type `text`): Applies a `WHERE` condition to the `SELECT`
  statement used during data transfer.

* `batchsize` (type `interger`): If set, the job will loop over source target to
  transfer data as a bunch of rows with intermediate `COMMIT` at batch
  completion.

Examples:

```sql
INSERT INTO config 
  (source, target, pkey, priority, parts, trunc, condition, batchsize) 
VALUES
-- t1 will be copied in a single operation
  ('source.t1', 'public.t1', 'id', 2, 1, true, null, null),
-- t2 will be dispatched to two jobs, each will insert data with a batch size of 200
  ('source.t2', 'public.t2', 'id', 1, 2, false, null, 200),
-- t3 will be copied with a condition that filters negative values
  ('source.t3', 'public.t3', 'id', 3, 1, true, 'value >= 0', null);
```

## API Usage

**plan(targets text[])** function

* `plan()` prepares a new stage by creating `stage`, `job` and `task` records
  and returns a set of `CALL copy()` statements in order of configured priority.

* `targets` parameter is an array of target relation names used as filter. If
  empty, all relations in the `config` table are used as targets.

* Using multiple parts (more than one) will result in multiple tasks (and tasks)
  being generated with a modulo condition to split into unique subsets.

**copy(job_id bigint)** procedure

* `copy()` handles `INSERT` statement build and execution, updates its own job
  record during bulk insertion batches and at the end with `success` or `failed`
  status.

* A table with `trunc` option will be truncated before bulk inserts.

* The same `copy(job_id)` statement can be executed several times without
  truncating a table with `trunc` option. This behavior is intended to resume
  the job from the last known sequence in case of unintentional interruption.

## Internal relations

**state** enum type

* `running`: a job is processing the data transfer

* `failed`: a error has been raised, the job stopped in the middle of a batch
  and rollbacked his current task.

* `pending`: a job has been prepared by the `plan()` procedure but the `copy()`
  has not been called yet.

* `completed`: a job has processed the data transfert successfully.

**report** view

* `stage_id` (type `bigint`): Identifier used to filter a specific stage.

* `target` (type `regclass`): Where the data goes to, relative to current
  `search_path`.

* `job_start` (type `timestamp`): Start time of the jobs, `null` if not started
  yet.

* `state` (type `state`): State of the jobs, `pending` by default.

* `rows` (type `numeric`): Number of rows processed by the jobs so far.

* `elapsed` (type `interval`): Cumulative elapsed time spent by the jobs.

* `rate` (type `numeric`): Calculated rate (rows per second) attached to a jobs,
  based on elapsed time and rows processed.

**stage** table

* `stage_id` (type `bigint`): A unique identifier spawned by calling `plan()`
  function.

* `ts` (type `timestamp`): Creation time of the stage.

**job** table

* `stage_id` (type `bigint`): Identifier of the stage that job belongs to.

* `job_id` (type `bigint`): A unique identifier to manipulate the job with
  `copy()` procedure.

* `part` (type `integer`): Subset identifier, start with 0.

* `lastseq` (type `bigint`): Last maximum value returned by the `INSERT`
  statement, based on the `pkey`, to resume the data transfer with new rows.

* `rows` (type `bigint`): Cumulative number of rows processed.

* `elapsed` (type `interval`): Cumulative elapsed time spent by the job.

* `ts` (type `timestamp`): Start time of the job, `null` if not started yet.

* `state` (type `state`): State of the job, `pending` by default.

## Hack

**Generate oracle_fdw key options for each foreign table**

When reading foreign tables through [oracle_fdw] extension, we should add a
special option on primary key column of the foreign table. It enforces index
usage on remote database when exporting data in a batch. 

[oracle_fdw]: https://github.com/laurenz/oracle_fdw

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
INSERT INTO config (target, source, pkey)
SELECT format('public.%I', c.relname), format('%I.%I', n.nspname, c.relname), a.attname
FROM pg_catalog.pg_foreign_table ft
INNER JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid
INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
INNER JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid,
LATERAL pg_catalog.pg_options_to_table(a.attfdwoptions) op
WHERE a.attnum > 0 AND NOT a.attisdropped
AND op.option_name = 'key' AND op.option_value = 'true';
```
