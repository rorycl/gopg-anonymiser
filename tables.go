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
	columnNames []string
	lines       int
	initialised bool
}

// ErrNoDumpTable reports that a dump table was not found
var ErrNoDumpTable = errors.New("not a dump table")

// ErrNotInterestingTable reports that a dump table wasn't interesting
var ErrNotInterestingTable = errors.New("not an interesting dump table")

// DumpTabler is an interface abstracting the functions of a DumpTable
// and ReferenceTable so that that two can be used interchangeably
type DumpTabler interface {
	// a line splitting function
	LineSplitter(line string) ([]string, bool)
	// show if the table has been initialised
	Inited() bool
	// list Columns
	ColumnNames() []string
}

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
	d.columnNames = strings.Split(matches[2], ", ")

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

// LineSplitter returns the fields of the requisite table and true if
// the table is still being read, false otherwise
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

// ColumnNames returns the DumpTable's column names
func (dt *DumpTable) ColumnNames() []string {
	return dt.columnNames
}

// ReferenceDumpTable is a Dump Table that keeps a record of its
// original values for back-referencing from other table rows. This
// allows for simple joins to be followed, for example if a users table
// is recorded in a RecordedDumpTable and a user has id 22 and the data
// in that row has been anonymised, any original or new valies in what
// was row 22 can be referenced
type ReferenceDumpTable struct {
	DumpTable
	originalRows []Row
	latestRows   []Row
}

// NewReferenceDumpTable creates a DumpTable wrapped with some
// additional fields for reference
func NewReferenceDumpTable(copyLine string, interestingTables []string) (*DumpTable, error) {

	dt, err := NewDumpTable(copyLine, interestingTables)
	if err != nil {
		return dt, fmt.Errorf("could not make reference dump table: %w", err)
	}
	return dt, nil
}

// addRow adds rows to either the original or latest row slices
func (rdt *ReferenceDumpTable) addRow(original bool, r Row) {
	if original {
		rdt.originalRows = append(rdt.originalRows, r)
		return
	}
	rdt.latestRows = append(rdt.latestRows, r)
	return
}

// getRefFieldValue attempts to to find a table's origValue in keyCol
// from rdt.originalRows, returning the new targetCol value from
// rdt.latestRows for that row
func (rdt *ReferenceDumpTable) getRefFieldValue(keyCol, origValue, targetCol string) (string, error) {

	keyColNo := -1
	for i, c := range rdt.ColumnNames() {
		if c == keyCol {
			keyColNo = i
			break
		}
	}
	if keyColNo == -1 {
		return "", fmt.Errorf("could not find referenced key column %s", keyCol)
	}

	targetColNo := -1
	for i, c := range rdt.ColumnNames() {
		if c == targetCol {
			targetColNo = i
			break
		}
	}
	if targetColNo == -1 {
		return "", fmt.Errorf("could not find referenced target column %s", targetCol)
	}

	// loop through the originalRows until the oldValue is found. If
	// found, use the offset to return the latest value in latestRows at
	// the same offset
	for i, row := range rdt.originalRows {
		if row.Columns[keyColNo] == origValue {
			return rdt.latestRows[i].Columns[targetColNo], nil
		}
	}
	return "", errors.New("could not find referenced key value")
}

// Row holds a line (represented by columnar data) from a postgresql
// dump file describing the contents of a postgreql table, together with
// the name of table, the column names and the line number (excluding
// header) within the table using a 1-indexed count
type Row struct {
	DumpTabler
	Columns []string
	lineNo  int
}

// NewRow constructs a new row
func NewRow(dt DumpTabler, columns []string, lineNo int) Row {
	return Row{
		DumpTabler: dt,
		Columns:    columns,
		lineNo:     lineNo,
	}
}

// colVal gets the value of a column
func (r *Row) colVal(column string) (string, error) {
	for i, cn := range r.ColumnNames() {
		if cn == column {
			return r.Columns[i], nil
		}
	}
	return "", fmt.Errorf("column %s not found", column)
}

// colno returns the ColumnNames offset of the named column, else an error
func (r *Row) colNo(column string) (int, error) {
	for i, c := range r.ColumnNames() {
		if c == column {
			return i, nil
		}
	}
	return -1, fmt.Errorf("could not find column %s", column)
}

// match determines returns true if a row column matches any
// map[column]values
func (r *Row) match(filterName string, where map[string]string) bool {
	for col, val := range where {
		colVal, err := r.colVal(col)
		if err != nil {
			continue
		}
		if colVal == val {
			return true
		}
	}

	return false
}
