package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// DumpTable  describes metadata about a table from pg_dump file
type DumpTable struct {
	TableName   string
	ColumnNames []string
	lines       int
	initialised bool
}

// ErrNoDumpTable reports that a dump table was not found
var ErrNoDumpTable = errors.New("not a dump table")

// ErrNotInterestingTable reports that a dump table wasn't interesting
var ErrNotInterestingTable = errors.New("not an interesting dump table")

// copy_regex is a regular expression to grab a COPY header from a
// pg_dump table COPY block
var copyRegex = regexp.MustCompile(`^COPY ([^ ]+) \(([^)]+)\) FROM stdin;`)

// NewDumpTable is used to initialise a dump table when given a "COPY"
// line from a pg_dump file, such as
//
//     COPY example_schema.events (id, flags, data) FROM stdin;
//
// but only if the name of the extracted table, including schema name,
// is in interestingTables
func NewDumpTable(copyLine string, interestingTables []string) (*DumpTable, error) {

	d := new(DumpTable)
	if !(strings.Contains(copyLine, "COPY ") && strings.Contains(copyLine, " FROM stdin;")) {
		return d, ErrNoDumpTable
	}
	matches := copyRegex.FindStringSubmatch(copyLine)
	if len(matches) != 3 {
		return d, fmt.Errorf("could not parse copy line %s", copyLine)
	}

	d.TableName = matches[1]
	d.ColumnNames = strings.Split(matches[2], ", ")

	// return early unless the table name is in interestingTables
	ok := false
	for _, it := range interestingTables {
		if it == d.TableName {
			ok = true
			break
		}
	}
	if !ok {
		return d, ErrNotInterestingTable
	}

	// mark the struct as initialised
	d.initialised = true

	return d, nil
}

// LineSplitter returns returns true and the fields of the requisite
// table if the table is still being read, false otherwise
func (dt *DumpTable) LineSplitter(line string) ([]string, bool) {

	s := []string{}

	// tables are terminated by a "\." line
	if line == `\.` {
		return s, false
	}
	dt.lines++
	s = strings.Split(line, "\t")
	return s, true
}

// Inited returns true if the DumpTable has been successfully
// initialised
func (dt *DumpTable) Inited() bool {
	return dt.initialised
}
