CREATE SCHEMA source;
CREATE TABLE source.t1 (
    id serial primary key,
    name text
);

-- name has a NOT NULL constraint to test edge cases
CREATE TABLE public.t1 (
    id serial primary key,
    name text not null
);

-- source.t2 is volountary larger to test copy in batches 
-- and through several jobs
CREATE TABLE source.t2 (
    id serial primary key,
    age int not null,
    name text not null
);

-- columns order differs from source.t2 and public.t2
-- ts column allows only smallint (2^8) values to test edge cases
CREATE TABLE public.t2 (
    id serial primary key,
    name text not null,
    age smallint not null
);

-- withkeywords table is used to test reserved keywords
-- in "extract" method
CREATE TABLE source.withkeywords (
    id serial primary key,
    "limit" integer not null
);

CREATE TABLE public.withkeywords (
    id serial primary key,
    "limit" integer not null
);

-- "dummy" is a table that exist but is not used in any job
CREATE TABLE public.dummy (
    id serial primary key,
    name text not null
);

INSERT INTO source.t1 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1, 100) i;

INSERT INTO source.t2 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1, 1000) i;

INSERT INTO source.withkeywords (id, "limit") VALUES (1, 1);
