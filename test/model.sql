CREATE SCHEMA source;
CREATE TABLE source.t1 (
    id serial primary key,
    name text
);
CREATE TABLE public.t1 (LIKE source.t1 INCLUDING ALL);

CREATE TABLE source.t2 (
    id serial primary key,
    ts timestamp default now(),
    name text
);
CREATE TABLE public.t2 (
    id serial primary key,
    name text,
    ts timestamp default now()
);
