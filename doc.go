/*
gopg-anonymiser

Note that this project is still in early development RCL April 2022

A simple tool for anonymising postgresql dump files from the Postgresql
`pg_dump` command, which uses row delete and column replacement filters
set out in a settings toml file.

The tool takes advantage of the structure of `COPY` lines in dump files,
that is those between a `COPY <schema>.<tablename> (...column list...)
FROM stdin;` and a `\.` terminating line, to separate the lines into
columns and to either remove lines or replace the columns specified.

Overview

The anonymiser can be used in a chain of pipes using `pg_dump` or
`pg_restore`, for example:

    pg_dump dbname -U <user> | \
        ./gopg-anonymise -s settings.toml

or to anonymise a pg\_dump custom format (`-Fc`) dump file to stdout:

    pg_restore -f - /tmp/test.sqlc | \
        ./gopg-anonymise -s setttings.toml

or dump, anonymise and load:

    pg_restore -f - /tmp/test.sqlc | \
        ./gopg-anonymise -s setttings.toml | \
            psql -d <dbname> -U <user>

Use the `-t` (testmode) flag to only show altered rows.

Running the programme

	Usage:
	  gopg-anonymise : a simple postgresql dump file anonymiser.

	Anonymise a postgresql dump file using a toml settings file setting out
	the deletion, or columnar uuid, string, file or reference replacement
	filters to use.

	gopg-anonymise -s <settings.toml> [-o output or stdout] [-t test] [Input]

	Application Options:
	  -s, --settings= settings toml file
	  -o, --output=   output file (otherwise stdout)
	  -t, --testmode  show only changed lines for testing

	Help Options:
	  -h, --help      Show this help message

	Arguments:
	  Input:          input file or stdin

An example settings file

A toml file is used to describe tables that should be anonymised. For
each table to be anonymised one or more filters may be provided.

Presently, apart from the row **delete** filter, four column replacement
filters are provided:

- uuid replaces: one or more columns with a new uuid

- string replace: replaces the data in one or more columns with
  replacement values

- file replace: replaces the data in one or more columns with
  corresponding lines in the source file. If the source file is
  exhausted, cycle the inputs starting from the first line of the
  source.

- reference replace: replaces data in one or more columns with the
  value from one or more values from a reference table, requiring the
  specification of a local and remote key to make matches along the
  lines of a simple foreign key lookup. Note that the
  reference table is read into memory. Reference tables track their
  original and new values so that, for example, a local table can update
  its `username` value from the new anonymised value in a `users` table.

Each filter can be qualified by `If` and `NotIf` filters which determine
if the filter should be run based on the contents of one or more columns
in the row. Conditionals match if any of their criteria are true.

The order of filters is important. Beware of changing a row ahead of
using a conditional.

	[["example_schema.events"]]
	filter = "delete"

	[["public.users"]]
	filter = "string replace"
	# column names must equal those in the dump file "COPY" load header
	# one or more columns must be provided
	columns = ["password"]
	# the number of replacements must equal the number of columns
	# give all users the same password
	replacements = ["$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC"]
	# don't replace if lastname column has the value "langoustine"
	notif = {"lastname" = "langoustine"}

	[["public.users"]]
	filter = "file replace"
	# column names must equal those in the dump file "COPY" load header
	# the number of columns in the source must equal the columns listed
	columns = ["firstname", "lastname"]
	source = "testdata/newnames.txt"

	[["public.users"]]
	filter = "uuid"
	columns = ["uuid"]

	[["public.users"]]
	filter = "file replace"
	columns = ["notes"]
	# cycle through 2 notes in the file
	source = "testdata/newnotes.txt"
	# only replace if the notes column is not null
	# Postgresql NULL columns must be recorded as '\N' or "\\N"
	notif = {"notes" = '\N'}

	[["public.fkexample"]]
	filter = "reference replace"
	# columns are the local table column names
	columns = ["firstname_materialized"]
	# replacements are the foreign table column names
	replacements = ["firstname"]
	# fklookups.0 is the localkey, .1 is the schema.table.column fk
	optargs = {"fklookup" = ["user_id", "public.users.id"]}

Example

This example uses the dump and settings toml files provided in the
testdata directory.

The events table contents are removed.

The fkexample table values in the firstname_materialized column are
replaced by the anonymised values in the users.firstname column
achieved by the match between public.fkexample.user_id and
public.users.id.

For the users table, the password column is replaced verbatim except for
user "langoustine", and all uuids are newly generated. The first name
and last name of each user is replaced from the file
"testdata/newnames.txt", although because there are only 5 entries in
the file, the 6th entry is recycled (with the firstname "zachary"). The
two notes in "testdata/newnotes.txt" are cycled through the three
entries which are not NULL.

	egrep -A 7 "COPY.*public.users" testdata/pg_dump.sql

	COPY public.users (id, firstname, lastname, password, uuid, notes) FROM stdin;
	1	ariadne	augustus	$2a$06$xyhc3ZN0KLlw4XSM8YypjueqptvViUdTBQq3m2as3QMZ/lL6gH6ie	6b1b3a33-484a-4870-b6ec-58a8d72fc306	\N
	2	james	joyce	$2a$06$YpMDzzGDmUz.tgGtkYotaeFnGliNymZTBIHPGPyCd8D9jXHLsnC/a	95ae2b5a-56a6-412d-b7af-e7d0eb1a412f	\N
	3	lucius	langoustine	$2a$06$cj4Coa76ZPud2KiFW4wPDuTL98N8p4mFjJoV5mJ2Id9.2QiAcJ6bO	db761046-e61e-4b5f-8dc5-64b89ed0dd77	\N
	4	biggles	barrymore	$2a$06$eS8/gKhuPcwdklVWwqgK0.9Z30Bk5hwveYBdyVQny1GwtnSoEEQ8C	f7a53cb0-454e-43f7-8559-e1e5097e1f3a	a 'note'
	5	asterix	a gaul	$2a$06$Llerb92vQ763qEX3e/v9WueqoCJdYu4F0mI65xo8Y1uif/vMTlsLq	46cebc75-8b9a-4666-94f3-8142e73c23d2	a "note", with commas, etc.
	6	wormtail	wyckenhof	$2a$06$BEOCQhB5i5zPkAqe2pKq5O6zJmafmwjxkn4NB0mek3w5o70ytkxzm	708fd360-34bb-4ea4-8096-71920bfa7809	a note with a tab here:"\t"
	\.

	./gopg-anonymise -t -s testdata/settings.toml testdata/pg_dump.sql

	COPY example_schema.events (id, flags, data) FROM stdin;
	COPY public.fkexample (id, user_id, firstname_materialized) FROM stdin;
	1	1	zachary
	2	3	xavier
	3	5	vanessa
	COPY public.users (id, firstname, lastname, password, uuid, notes) FROM stdin;
	1	zachary	zaiden	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	6a474264-5cd7-4184-a1ed-9049ed4aecaf	\N
	2	yael	yaeger	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	f3da4fee-c05a-447f-a619-a4b0c626c456	\N
	3	xavier	xander	$2a$06$.d8FVKIVagQaHU.6ouHGKegL85H8.cFIvXDNGC/wb8dXAWt3fmukq	6837753f-4dc9-4e95-9d14-375aa62cd021	\N
	4	william	williamson	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	242704da-6ba9-47b7-a84e-e2216b713324	this is a second note\twith a tab
	5	vanessa	vaccarelli	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	913aae23-d9df-4c66-9f93-9ce59f94c907	this is the first note
	6	zachary	zaiden	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	3395c1a1-d9ea-4803-81ba-65333a7698e3	this is a second note\twith a tab

Licence

This software is provided under an MIT licence.

*/

package main
