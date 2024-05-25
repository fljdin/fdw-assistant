CREATE SCHEMA source;
CREATE TABLE source.t1 (
    id serial primary key,
    name text
);

INSERT INTO source.t1 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1, 100) i;

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

INSERT INTO source.t2 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1, 1000) i;

-- columns order differs from source.t2 and public.t2
-- ts column allows only smallint (2^8) values to test edge cases
CREATE TABLE public.t2 (
    id serial primary key,
    name text not null,
    age smallint not null
);

-- source.t3 contains negative values that we wont copy
CREATE TABLE source.t3 (
    id serial primary key,
    value smallint not null
);

INSERT INTO source.t3 (id, value)
    SELECT i, i - 100 FROM generate_series(1, 199) i;

CREATE TABLE public.t3 (
    id serial primary key,
    value smallint not null CHECK (value >= 0)
);

-- withkeywords table is used to test reserved keywords
-- in "extract" method
CREATE TABLE source.withkeywords (
    id serial primary key,
    "limit" integer not null
);

INSERT INTO source.withkeywords (id, "limit") VALUES (1, 1);

CREATE TABLE public.withkeywords (
    id serial primary key,
    "limit" integer not null
);

-- "dummy" is a table that exist but is not used in any job
CREATE TABLE public.dummy (
    id serial primary key,
    name text not null
);
