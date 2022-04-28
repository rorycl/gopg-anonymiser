package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
)

// RowFilterer is the interface that any row filter needs to fulfil to
// filter a row, perhaps on a column basis, returning a row to allow
// chaining of filters
type RowFilterer interface {
	// FilterName returns the name of the filter
	FilterName() string
	// Filter performs filtering on a row of data
	Filter(r Row) (Row, error)
	// setRefDumpTable sets the foreign dump table, only needed for
	// reference replace filters
	setRefDumpTable(rt RefTableRegister)
	// getExternalTables retrieves the name of any foreign dump table
	getRefDumpTable() string
}

// filterName is the base filter type name, embedded in each filter
// struct
type filterName string

// FilterName returns the name of the filter type
func (f filterName) FilterName() string {
	return string(f)
}

// setRefDumpTable in the general case does notthing
func (f *filterName) setRefDumpTable(rt RefTableRegister) {
	return
}

// getRefDumpTable returns the name of any reference dump table, if any
func (f *filterName) getRefDumpTable() string {
	return ""
}

// DeleteFilter removes all lines
type DeleteFilter struct {
	filterName
}

// NewDeleteFilter makes a new DeleteFilter
func NewDeleteFilter() (*DeleteFilter, error) {
	return &DeleteFilter{"delete"}, nil
}

// Filter returns an empty row
func (f DeleteFilter) Filter(r Row) (Row, error) {
	var rr Row
	return rr, nil
}

// replaceByColumnFilter replaces a column named "Column" with the
// provided replacement string
type replaceByColumnFilter struct {
	filterName
	Column      string
	Replacement string
	whereTrue   map[string]string
	whereFalse  map[string]string
}

// newReplaceByColumnFilter makes a new replaceByColumnFilter, which
// should only be called by a multicolumn replace filter
func newReplaceByColumnFilter(column, replacement string, whereTrue, whereFalse map[string]string) (*replaceByColumnFilter, error) {
	f := &replaceByColumnFilter{
		filterName:  "string replace",
		Column:      column,
		Replacement: replacement,
		whereTrue:   whereTrue,
		whereFalse:  whereFalse,
	}
	if column == "" {
		return f, errors.New("string replacer: column name cannot be empty")
	}
	return f, nil
}

// Filter replaces a column with a fixed string replacement
func (f *replaceByColumnFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.lineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.FilterName(), f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.FilterName(), f.whereFalse) == true {
		return r, nil
	}

	// find the column number to replace if it has not been initialised
	colNo, err := r.colNo(f.Column)
	if err != nil {
		return r, fmt.Errorf("column %s string replace: %w", f.Column, err)
	}

	// replace the column contents
	r.Columns[colNo] = f.Replacement
	return r, nil
}

// fileByColumnFilter reads the contents of file into the struct and
// uses this to replace the contents of the designated column
type fileByColumnFilter struct {
	filterName
	Column       string
	Replacements []string
	whereTrue    map[string]string
	whereFalse   map[string]string
	matches      int
}

// newFileByColumnFilter makes a new fileByColumnFilter, which should
// only be called by a multi-columnar FileFilter
func newFileByColumnFilter(column string, fh io.Reader, whereTrue, whereFalse map[string]string) (*fileByColumnFilter, error) {

	f := &fileByColumnFilter{
		filterName: "file replace",
		Column:     column,
		whereTrue:  whereTrue,
		whereFalse: whereFalse,
	}
	if column == "" {
		return f, errors.New("file replacer: column name cannot be empty")
	}

	// append the replacement lines to the replacement slice
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		t := scanner.Text()
		if strings.Contains(t, "\t") {
			return f, fmt.Errorf("file replacer: source for %s contains a tab", f.Column)
		}
		f.Replacements = append(f.Replacements, t)
	}
	// return an error if the scanner failed
	if err := scanner.Err(); err != nil {
		return f, err
	}

	if len(f.Replacements) == 0 {
		return f, fmt.Errorf("file replacer: no replacements found for %s", f.Column)
	}

	return f, nil
}

// Filter replaces a column with the replacement indexed by the provided
// row number from the list of replacements. If the list of replacements
// has been exhausted, start from the top again
func (f *fileByColumnFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.lineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.FilterName(), f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.FilterName(), f.whereFalse) == true {
		return r, nil
	}

	// find the column number to replace if it has not been initialised
	colNo, err := r.colNo(f.Column)
	if err != nil {
		return r, fmt.Errorf("column %s file replacer error: %w", f.Column, err)
	}

	// replace the column contents with the replacement equalling the (1
	// indexed) row number of the input with the modulo of the length of
	// the replacements
	r.Columns[colNo] = f.Replacements[(r.lineNo-1)%len(f.Replacements)]
	return r, nil
}

// UUIDFilter replaces the column with the output of a UUID
// generation function
type UUIDFilter struct {
	filterName
	Columns    []string
	whereTrue  map[string]string
	whereFalse map[string]string
}

// NewUUIDFilter makes a new UUIDFilter
func NewUUIDFilter(columns []string, whereTrue, whereFalse map[string]string) (*UUIDFilter, error) {

	f := UUIDFilter{
		filterName: "uuid replace",
		Columns:    columns,
		whereTrue:  whereTrue,
		whereFalse: whereFalse,
	}

	if len(columns) == 0 {
		return &f, errors.New("uuid replace: at least one column must be specified")
	}

	return &f, nil
}

// Filter replaces a column with the replacement indexed by the provided
// row number with a uuid
func (f *UUIDFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.lineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.FilterName(), f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.FilterName(), f.whereFalse) == true {
		return r, nil
	}

	// find the column number to replace between the row r and filter f
	changed := 0
	for i, rc := range r.ColumnNames() {
		for _, cn := range f.Columns {
			if rc == cn {
				changed++
				r.Columns[i] = uuid.New().String()
				break
			}
		}
	}

	if changed != len(f.Columns) {
		return r, errors.New("uuid replacer: could not find all columns in UUIDFilter")
	}

	return r, nil
}

// ReplaceFilter allows multiple columns to be replaced by fixed
// strings, described by a slice of replacements. ReplaceFilter uses
// replaceByColumnFilter to do its work
type ReplaceFilter struct {
	filterName
	Columns      []string
	Replacements []string
	filters      []RowFilterer
}

// NewReplaceFilter creates a new ReplaceFilter, registering one or more
// replaceByColumnFilter filters to do the work of replacing each
// column.
func NewReplaceFilter(columns, replacements []string, whereTrue, whereFalse map[string]string) (*ReplaceFilter, error) {
	f := &ReplaceFilter{
		filterName:   "multi string replace",
		Columns:      columns,
		Replacements: replacements,
		filters:      []RowFilterer{},
	}
	if len(columns) == 0 {
		return f, errors.New("multi string replace: at least one column must be specified")
	}
	if len(columns) != len(replacements) {
		return f, errors.New("multi string replace: number of columns and replacements must be the same")
	}
	for i, c := range f.Columns {
		cf, err := newReplaceByColumnFilter(c, f.Replacements[i], whereTrue, whereFalse)
		if err != nil {
			return f, fmt.Errorf("multi string init error %w", err)
		}
		f.filters = append(f.filters, cf)
	}
	return f, nil
}

// Filter replaces column values with a fixed string replacement using
// one or more replaceByColumnFilter filters
func (f ReplaceFilter) Filter(r Row) (Row, error) {
	// if there is no line number the previous filter may have stopped
	// processing
	if r.lineNo == 0 {
		return r, nil
	}
	for _, f := range f.filters {
		r, err := f.Filter(r)
		if err != nil {
			return r, fmt.Errorf("multi string filter error: %w", err)
		}
	}
	return r, nil
}

// FileFilter replaces a number of columns in a table with replacements
// from a tab delmited file (typically a postgres dump file). The actual
// work of this filter is performed by a set of fileByColumnFilter
// filter
type FileFilter struct {
	filterName
	Columns      []string
	Replacements []bytes.Buffer
	filters      []RowFilterer
}

// NewFileFilter creates a new FileFilter
func NewFileFilter(columns []string, fh io.Reader, whereTrue, whereFalse map[string]string) (*FileFilter, error) {
	f := &FileFilter{
		filterName: "multi file replace",
		Columns:    columns,
		filters:    []RowFilterer{},
	}
	if len(columns) == 0 {
		return f, errors.New("multi file replace: at least one column must be specified")
	}

	f.Replacements = make([]bytes.Buffer, len(f.Columns))

	// scan the provided reader resource into columns by splitting on
	// tab, appending each to the numbered buffer, erroring if the
	// number of columns made by splitting is more than Columns
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) > len(f.Columns) {
			return f, fmt.Errorf(
				"multi file replacement error: file column number %d greater than requested %d",
				len(cols), len(f.Columns),
			)
		}
		for i, c := range cols {
			f.Replacements[i].WriteString(c + "\n")
		}
	}
	for i, c := range f.Columns {
		replReader := bytes.NewReader(f.Replacements[i].Bytes())
		cf, err := newFileByColumnFilter(c, replReader, whereTrue, whereFalse)
		if err != nil {
			return f, fmt.Errorf("multi file init error %w", err)
		}
		f.filters = append(f.filters, cf)
	}
	return f, nil
}

// Filter replaces column values with values read from one or more
// buffers providing data to one or more fileByColumnFilter filters
func (f FileFilter) Filter(r Row) (Row, error) {
	// if there is no line number the previous filter may have stopped
	// processing
	if r.lineNo == 0 {
		return r, nil
	}
	for _, f := range f.filters {
		r, err := f.Filter(r)
		if err != nil {
			return r, fmt.Errorf("multi string file error: %w", err)
		}
	}
	return r, nil
}

// ReferenceFilter replaces a number of columns in a table with
// replacements from another table, as a very simple foreign key lookup using
// the local key localKey and foreignKey (qualified in schema.table.column
// format)
//
// Given a localKey of "key" and remoteKey of "schema.table.key" source columns
// of ["user", "score"] and target columns of ["firstname", "average"], these
// tables:
//
//	local:  key user  score ace
//	        1   adam  30    *
//		    3   james 10
//		    2   billy 20
//
//	remote: key firstname average
//	        1   jenny     20
//			2   sue       20
//			3   trish     20
//
// will transform the local table into
//
//	local:  key user  score ace
//	        1   jenny 20    *
//		    3   trish 20
//		    2   sue   20
//
// Settings:
// OptArgs[fklookup][0] is the local key column
// OptArgs[fklookup][1] is the foreign key column
// fklookup][1] keys need to be in the format schema.table.column
type ReferenceFilter struct {
	filterName
	Columns      []string // local target columns
	Replacements []string // remote data source columns
	whereTrue    map[string]string
	whereFalse   map[string]string

	localKey     string
	foreignKey   string              // the key column name in the referenced table in schema.table.column format
	foreignTable string              // the referenced table name
	refDumpTable *ReferenceDumpTable // pointer to the dump table referred to by foreignTable
}

// NewReferenceFilter makes a new ReferenceFilter
func NewReferenceFilter(columns, replacements []string, whereTrue, whereFalse map[string]string, localKey, foreignKey string) (ReferenceFilter, error) {

	f := ReferenceFilter{
		filterName:   "reference replace",
		Columns:      columns,
		Replacements: replacements,
		whereTrue:    whereTrue,
		whereFalse:   whereFalse,
		localKey:     localKey,
		foreignKey:   foreignKey,
	}
	if len(columns) == 0 {
		return f, errors.New("reference filters need at least one column specified")
	}
	if len(columns) != len(replacements) {
		return f, fmt.Errorf("column length %d != replacement length %d", len(columns), len(replacements))
	}
	// check that the local and foreignKey keys follows the expected format
	if len(strings.Split(localKey, ".")) != 1 {
		return f, fmt.Errorf("reference filter local key must not have schema qualification, got %s", localKey)
	}
	if len(strings.Split(foreignKey, ".")) != 3 {
		return f, fmt.Errorf("reference filter foreign key requires schema.table.column format, got %s", foreignKey)
	}
	parts := strings.Split(foreignKey, ".")
	f.foreignTable = parts[0] + "." + parts[1]
	f.foreignKey = parts[2] // reassign
	return f, nil
}

// SetRefDumpTable adds the named reference dump table to the filter
// struct. This happens by necessity after the ReferenceFilter has been
// initialised
func (f *ReferenceFilter) setRefDumpTable(rt RefTableRegister) {
	for k, v := range rt {
		if k == f.foreignTable {
			f.refDumpTable = v
			break
		}
	}
	return
}

// getRefDumpTable returns the name of any reference dump table, if any
func (f *ReferenceFilter) getRefDumpTable() string {
	return f.foreignTable
}

// Filter replaces a column with the replacement indexed by the provided
// row number with replacements in the external table referenced by
func (f *ReferenceFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.lineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.FilterName(), f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.FilterName(), f.whereFalse) == true {
		return r, nil
	}

	// abort if the reference dump table is nil
	if f.refDumpTable == nil {
		return r, errors.New("reference filter error: no reference dump table")
	}

	// for this row extract the local key value and then supply the remote
	// target column name, to determine the remote matching row from which the
	// target column value is extracted. Concepts:
	// local key     : f.localKey    (supplies value for matching remote key)
	// local key val : keyValue      (used for matching remote)
	// remote key    : f.foreignKey  (supply name of key)
	// local column  : targetColNo   (column number of the local column to replace)
	// remote column : remoteColName (column name of the remote source)

	keyValue, err := r.colVal(f.localKey)
	if err != nil {
		fmt.Errorf("reference filter cannot resolve key value: %w", err)
	}

	for i, colName := range f.Columns {

		targetColNo, err := r.colNo(colName)
		if err != nil {
			fmt.Errorf("reference filter: cannot resolve column name %s: %w", colName, err)
		}
		remoteColName := f.Replacements[i]

		v, err := f.refDumpTable.getUpdatedFieldValue(
			f.foreignKey, keyValue, remoteColName,
		)
		if err != nil {
			return r, fmt.Errorf("reference filter could not retrieve value: %w", err)
		}
		r.Columns[targetColNo] = v
	}

	return r, nil
}
