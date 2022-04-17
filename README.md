# gopg-anonymiser

_Note that this project is still in early development_ RCL April 2022

A simple tool for anonymising postgresql dump files from the Postgresql
`pg_dump` command, which uses row delete and column replacement filters
set out in a settings toml file.

## Overview

The anonymiser can be used in a chain of pipes using `pg_dump` or
`pg_restore`, for example:

    pg_dump dbname -U <user> | \
        ./gopg-anonymise -s settings.toml

or:

    pg_restore -f - /tmp/test.sqlc | \
        ./gopg-anonymise -s setttings.toml

or dump, anonymise and load:

    pg_restore -f - /tmp/test.sqlc | \
        ./gopg-anonymise -s setttings.toml | \
            psql -d <dbname> -U <user>

Use the `-t` (testmode) flag to only show altered rows.

## Running the programme

	./gopg-anonymise -h

	Usage:
	  gopg-anonymise : a simple postgresql dump file anonymiser.

	Anonymise a postgresql dump file using a toml settings file setting out
	the deletion, or columnar uuid, string or file filters to use.

	gopg-anonymise -s <settings.toml> [-o output or stdout] [-t test] [Input]

	Application Options:
	  -s, --settings= settings toml file
	  -o, --output=   output file (otherwise stdout)
	  -t, --testmode  show only changed lines for testing

	Help Options:
	  -h, --help      Show this help message

	Arguments:
	  Input:          input file or stdin

## An example settings file

A toml file is used to describe tables that should be anonymised. For
each table to be anonymised one or more filters may be provided. The
filters are either a delete filter (which removes all rows in the table)
or single or multi-column filters.

Presently, apart from the delete filter, three per-column string
replacement filters are provided. The `uuid` filter replaces the named
columns with new uuids. The `string replace` filter replaces the named
columns with the replacement strings. Finally, the `file replace` filter
replaces the named columns with corresponding lines in the source file.
If the source file is exhausted, numbering begins again at the top of
the source.

```toml
[["example_schema.events"]]
filter = "delete"

[["public.users"]]
filter = "file replace"
columns = ["name"]
source = "testdata/newnames.txt"

[["public.users"]]
filter = "string replace"
columns = ["password"]
# give all users the same password
replacements = ["$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC"]

[["public.users"]]
filter = "file replace"
columns = ["notes"]
# cycle through 3 example notes
source = "testdata/newnotes.txt"
```

## Example

This example uses the dump and settings toml files provided in the testdata directory.

```
egrep -A 7 "COPY.*public.users" testdata/pg_dump.sql

COPY public.users (id, name, age, password, notes) FROM stdin;
1	ariadne	22	$2a$06$6NX0WOwJ7i57BXi7E8bR.OS3C1/B/C3O3s9O7XCdxQtYKi6HY/K8G	\N
2	james	17	$2a$06$jipeOgnD0Ibpa5hyTgtnwuwrYFVlwEitl8plrC7vJ4W8uN76i0WUK	\N
3	lucius	77	$2a$06$zT9WAgHzuKQkhq6ghnA9VuhK11t3pvx7AEHz6ed5NhpUwaJocAfe2	\N
4	biggles	8	$2a$06$ICaf31zcP4VyxBHqnmd3VefOhQUurllaAZqQk2Cq8yVXHmNOj9RJe	a 'note'
5	asterix	7	$2a$06$fokqEHm2.Pxsa1wDDW9kg.QxTyh4X90TX05oI7tr1b1OtuX.SFbLm	a "note", with commas, etc.
6	wormtail	99	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	a note with a tab here:"\t"
\.

./gopg-anonymise -t -s testdata/settings.toml testdata/pg_dump.sql

COPY public.users (id, name, age, password, notes) FROM stdin;
1	isra	22	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	this is the first note
2	dilara	17	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	this is a second note\twith a tab
3	isra	77	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	a third note
4	dilara	8	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	this is the first note
5	isra	7	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	this is a second note\twith a tab
6	dilara	99	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	a third note
```

## Licence

This software is provided under an MIT licence.
