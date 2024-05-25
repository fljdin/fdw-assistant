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

INSERT INTO source.t1 (id, name) 
    SELECT i, 'name' || i FROM generate_series(1, 100) i;

INSERT INTO source.t2 (id, age, name)
    SELECT i, i, 'name' || i FROM generate_series(1, 1000) i;
