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

// RefTableRegister is a register of reference tables
type RefTableRegister map[string]*ReferenceDumpTable

// ErrNoDumpTable reports that a dump table was not found
var ErrNoDumpTable = errors.New("not a dump table")

// ErrNotInterestingTable reports that a dump table wasn't interesting
var ErrNotInterestingTable = errors.New("not an interesting dump table")

// ErrIsRefDumpTable reports that a reference dump table was found in a
// normal dump table context
var ErrIsRefDumpTable = errors.New("reference dump table found in a normal context")

// ErrIsNormalDumpTable reports that a dump table was found in a
// reference dump table context
var ErrIsNormalDumpTable = errors.New("normal dump table found in a reference context")

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
// is in the tableFilters argument, and whether or not it is called in
// a refContext mode (i.e. reference context)
func NewDumpTable(copyLine string, refContext bool, tf tableFilters) (*DumpTable, error) {

	d := new(DumpTable)

	if !(strings.Contains(copyLine, "COPY ") && strings.Contains(copyLine, " FROM stdin;")) {
		return d, ErrNoDumpTable
	}
	matches := copyRegex.FindStringSubmatch(copyLine)
	if len(matches) != 3 {
		return d, fmt.Errorf("could not parse copy line %s with parts %d", copyLine, len(matches))
	}

	d.TableName = matches[1]
	d.columnNames = strings.Split(matches[2], ", ")

	// If in refContext mode, return ErrIsNormalDumpTable unless the
	// table is in tf.refTableNames.
	if refContext == true {
		ex := tf.getReferenceTables()
		if _, ok := ex[d.TableName]; !ok {
			return d, ErrNotInterestingTable
		}
	} else {
		filters := tf.getTableFilters(d.TableName)
		if len(filters) == 0 {
			return d, ErrNotInterestingTable
		}
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
// was row 22 can be referenced. The index map is a map of column names
// to values in that column and the row number (in both originalRows and
// latestRows).
type ReferenceDumpTable struct {
	*DumpTable
	rowIndex     map[string]map[string]int // column name to value to row number
	originalRows []Row
	latestRows   []Row
}

// NewReferenceDumpTable creates a DumpTable wrapped with some
// additional fields for reference
func NewReferenceDumpTable(copyLine string, tf tableFilters) (*ReferenceDumpTable, error) {

	var rdt ReferenceDumpTable
	var err error
	rdt.DumpTable, err = NewDumpTable(copyLine, true, tf)
	if err != nil {
		return &rdt, err // err comes in several flavours, eg ErrNotInterestingTable
	}
	rdt.originalRows = []Row{}
	rdt.latestRows = []Row{}
	rdt.rowIndex = map[string]map[string]int{}

	return &rdt, nil
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

// getColNo returns the column number of a column
func (rdt *ReferenceDumpTable) getColNo(keyCol string) (int, error) {
	for i, c := range rdt.columnNames {
		if c == keyCol {
			return i, nil
		}
	}
	return -1, fmt.Errorf("could not find column %s", keyCol)
}

// buildIndex builds an index map for a particular column based on
// values in rdt.latestRows. Note that duplicate row values are not
// supported
func (rdt *ReferenceDumpTable) buildIndex(keyCol string) error {

	if len(rdt.originalRows) == 0 {
		return errors.New("original rows has 0 length; index creation not possible")
	}

	// initialise rdt.rowIndex if necessary
	if len(rdt.rowIndex) == 0 {
		rdt.rowIndex = make(map[string]map[string]int)
	}

	// initialise map for this column
	rdt.rowIndex[keyCol] = make(map[string]int)
	for i, l := range rdt.originalRows {

		val, err := l.colVal(keyCol)
		if err != nil {
			return fmt.Errorf("could not find value for column %s", keyCol)
		}
		rdt.rowIndex[keyCol][val] = i
	}
	return nil
}

// getUpdatedFieldValue looks up the row number of the original value in
// keyCol and returns the new value for targetCol at that row
func (rdt *ReferenceDumpTable) getUpdatedFieldValue(keyCol, originalValue, targetCol string) (string, error) {

	// if the index doesn't exist, build it
	if len(rdt.rowIndex[keyCol]) == 0 {
		rdt.buildIndex(keyCol)
	}

	// find the map for this column
	colMap, ok := rdt.rowIndex[keyCol]
	if !ok {
		return "", fmt.Errorf("could not find column %s in rowindex", keyCol)
	}

	// find the index of the field value for this column
	index, ok := colMap[originalValue]
	if !ok {
		return "", fmt.Errorf("could not find value %s for column %s", originalValue, keyCol)
	}

	// get the column number of the target column
	tcol, err := rdt.getColNo(targetCol)
	if err != nil {
		return "", fmt.Errorf("could not find referenced field: %w", err)
	}

	// return the value for the row of the latest values at index in the
	// target column
	return rdt.latestRows[index].Columns[tcol], nil
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
