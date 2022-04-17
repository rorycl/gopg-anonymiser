/*
gopg-anonymiser is a simple tool for anonymising postgresql dump files
from the Postgresql `pg_dump` command, which uses row delete and column
replacement filters set out in a settings toml file.

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

An example settings file

A toml file is used to describe tables that should be anonymised. For
each table to be anonymised one or more filters may be provided.

Presently, apart from the row delete filter, three column replacement
filters are provided:

- *uuid* replaces one or more columns with a new uuid

- *string replace* replaces the data in one or more columns with
  replacement values

- *file replace* replaces the data in one or more columns with
  corresponding lines in the source file. If the source file is
  exhausted, cycle the inputs starting from the first line of the
  source.

Example file

	[["example_schema.events"]]
	filter = "delete"

	[["public.users"]]
	filter = "file replace"
	# column names must equal those in the dump file "COPY" load header
	# the number of columns in the source must equal the columns listed
	columns = ["firstname", "lastname"]
	source = "testdata/newnames.txt"

	[["public.users"]]
	filter = "string replace"
	# column names must equal those in the dump file "COPY" load header
	# one or more columns must be provided
	columns = ["password"]
	# the number of replacements must equal the number of columns
	# give all users the same password
	replacements = ["$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC"]

	[["public.users"]]
	filter = "uuid"
	columns = ["uuid"]

	[["public.users"]]
	filter = "file replace"
	columns = ["notes"]
	# cycle through 3 example notes
	source = "testdata/newnotes.txt"

Example

This example uses the dump and settings toml files provided in the
testdata directory.

The password column is replaced verbatim, the uuids are regenerated and
the events table is effectively truncated. The user list is cycled for
the 6th entry as there are only 5 entries in `testdata/newnames.txt`,
and the three notes in `testdata/newnotes.txt` are similarly cycled.

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
	COPY public.users (id, firstname, lastname, password, uuid, notes) FROM stdin;
	1	zachary	zaiden	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	9c88e633-29a2-4d3d-9b82-a9203b0e67a0	this is the first note
	2	yael	yaeger	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	cd3f24e7-24d5-4a74-8820-e9abb62a62e6	this is a second note\twith a tab
	3	xavier	xander	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	69d90d89-9214-4b90-9c68-d7ec8cfec52c	a third note
	4	william	williamson	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	4753a95f-df28-4ca0-8d6a-f7adb65f4d23	this is the first note
	5	vanessa	vaccarelli	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	90f77e56-3f28-4eaf-b6e8-18ce45b11588	this is a second note\twith a tab
	6	zachary	zaiden	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	f8d84316-90e0-43af-85fc-d6574f8c6a60	a third note

Licence

This software is provided under an MIT licence.

*/

package main
