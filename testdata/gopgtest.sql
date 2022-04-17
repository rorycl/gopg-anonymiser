/*
Test postgresql table and contents file, for dumping via pg_dump

Note that to use the uuid_generate_v4 function the uuid-ossp extension needs to
be loaded into the database by the superuser, eg:

    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

*/



-- a users table
DROP TABLE IF EXISTS public.users;

CREATE TABLE public.users (
    id INT GENERATED BY DEFAULT AS IDENTITY
    ,firstname TEXT NOT NULL
    ,lastname TEXT NOT NULL
    ,password TEXT NOT NULL -- use crypt(in_password, gen_salt('bf'))
    ,uuid UUID NOT NULL DEFAULT uuid_generate_v4()
    ,notes TEXT
);


-- a new schema
DROP SCHEMA IF EXISTS example_schema CASCADE;

CREATE schema IF NOT EXISTS example_schema;

-- a table in the new schema
CREATE TABLE example_schema.events (
    id INT GENERATED BY DEFAULT AS IDENTITY
    ,flags TEXT[]
    ,data jsonb
);

/*
as postgres
create schema extensions;
create extension pgcrypto with schema extensions;
grant usage on schema extensions to <user>
*/

SET search_path = public, example_schema, extensions;

INSERT INTO users (firstname, lastname, password) VALUES
('ariadne', 'augustus', crypt('test1', gen_salt('bf')))
,('james', 'joyce', crypt('test2', gen_salt('bf')))
,('lucius', 'langoustine', crypt('test3', gen_salt('bf')))
;

INSERT INTO users (firstname, lastname, password, notes) VALUES
('biggles', 'barrymore', crypt('test4', gen_salt('bf')), 'a ''note''')
,('asterix', 'a gaul', crypt('test5', gen_salt('bf')), 'a "note", with commas, etc.')
,('wormtail', 'wyckenhof', crypt('test6', gen_salt('bf')), 'a note with a tab here:"	"')
;

INSERT INTO events (flags, data) VALUES
(array['flag1', 'flag2'], '{"a": "b"}')
,(array['flag1,a', 'flag2,b'], '{"a": "c,b", "b": [1, 0]}')
,(array['flag3'], '{"c": null}')
,(array['flag3	tab'], '{"d": "x  y"}')
;
