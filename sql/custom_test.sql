CREATE SCHEMA source;

create table source.clients (
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

COMMENT ON COLUMN source.clients.is_active IS 'bool(%s)';

COMMENT ON COLUMN source.clients.is_imported IS 'bool(%s)';

create table public.clients (
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

create table source.documents (request text);

COMMENT ON COLUMN source.documents.request IS '%s::json';

create table public.documents (request json);

INSERT INTO config (source, target, pkey, priority, parts, trunc, condition, batchsize) VALUES
  ('source.clients', 'public.clients', null, 1, 1, true, null, null),
  ('source.documents', 'public.documents', null, 2, 1, true, null, null);

SELECT * FROM config;

SELECT invocation FROM plan() \gexec
