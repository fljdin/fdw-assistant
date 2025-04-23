CREATE SCHEMA source;

CREATE TABLE source.clients (
  id bigint not null,
  firstname varchar(255),
  lastname varchar(255) not null,
  created_at timestamp(0),
  updated_at timestamp(0),
  is_active smallint,
  is_imported smallint,
  source varchar(255),
  updated_by bigint,
  imported_at timestamp(0),
  deleted_at timestamp(0)
);

ALTER TABLE source.clients
ALTER COLUMN deleted_at TYPE varchar;

COMMENT ON COLUMN source.clients.deleted_at IS 'REPLACE(%s, ''0000-00-00'', ''1970-01-01'')::timestamp';

COMMENT ON COLUMN source.clients.is_active IS 'bool(%s)';

COMMENT ON COLUMN source.clients.is_imported IS 'bool(%s)';

CREATE TABLE public.clients (
  id bigint not null,
  firstname varchar(255),
  lastname varchar(255) not null,
  created_at timestamp(0),
  updated_at timestamp(0),
  imported_at timestamp(0),
  deleted_at timestamp(0),
  updated_by bigint,
  is_active boolean,
  is_imported boolean,
  source varchar(255)
);

CREATE TABLE source.documents (request text);

COMMENT ON COLUMN source.documents.request IS '%s::json';

CREATE TABLE public.documents (request json);

CREATE TABLE source."MixedCaseTable" ("MixedCaseColumn" smallint);

CREATE TABLE public."MixedCaseTable" ("MixedCaseColumn" smallint);

CREATE TABLE source.dummy ();

CREATE TABLE public.dummy ();

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
  ('source.clients', 'public.clients', null, 1, 1, true, null, null),
  ('source.documents', 'public.documents', null, 2, 1, true, null, null),
  ('source."MixedCaseTable"', 'public."MixedCaseTable"', null, 3, 1, true, null, null),
  ('source.dummy', 'public.dummy', null, 4, 1, true, 'must fail', null);

SELECT * FROM config;

SELECT invocation FROM plan() \gexec

SELECT target, state, rows, total FROM report WHERE stage_id = 1 ORDER BY job_start;
