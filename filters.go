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

// ErrRowMatchFalse reports that a row did not match conditions
var ErrRowMatchFalse = errors.New("the row did not match the conditions")

// ErrRowNoMatchFalse reports that a row did not match noMatch conditions
// so if there is a condition of a null column and the inspected column
// is not null, this will raise ErrRowNoMatchFalse
var ErrRowNoMatchFalse = errors.New("the row did not match the conditions")

// Row holds a line (represented by columnar data) from a postgresql
// dump file describing the contents of a postgreql table, together with
// the name of table, the column names and the line number (excluding
// header) within the table using a 1-indexed count
type Row struct {
	TableName   string
	Columns     []string
	ColumnNames []string
	LineNo      int
}

// colVal gets the value of a column
func (r *Row) colVal(column string) (string, error) {
	for i, cn := range r.ColumnNames {
		if cn == column {
			return r.Columns[i], nil
		}
	}
	return "", fmt.Errorf("column %s not found", column)
}

// colno returns the ColumnNames offset of the named column, else an error
func (r *Row) colNo(column string) (int, error) {
	for i, c := range r.ColumnNames {
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

// RowFilterer is the interface that any row filter needs to fulfil to
// filter a row, perhaps on a column basis, returning a row to allow
// chaining of filters
type RowFilterer interface {
	// FilterName returns the name of the filter
	FilterName() string
	// Filter performs filtering on a row of data
	Filter(r Row) (Row, error)
}

// DeleteFilter removes all lines
type DeleteFilter struct {
	Typer string
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

// FilterName returns the Typer information about the DeleteFilter
func (f DeleteFilter) FilterName() string {
	return f.Typer
}

// replaceByColumnFilter replaces a column named "Column" with the
// provided replacement string
type replaceByColumnFilter struct {
	Typer       string
	Column      string
	Replacement string
	whereTrue   map[string]string
	whereFalse  map[string]string
}

// newReplaceByColumnFilter makes a new replaceByColumnFilter, which
// should only be called by a multicolumn replace filter
func newReplaceByColumnFilter(column, replacement string, whereTrue, whereFalse map[string]string) (*replaceByColumnFilter, error) {
	f := &replaceByColumnFilter{
		Typer:       "string replace",
		Column:      column,
		Replacement: replacement,
		whereTrue:   whereTrue,
		whereFalse:  whereFalse,
	}
	if column == "" {
		return f, errors.New("string replacer: column name cannot be empty")
	}
	if replacement == "" {
		return f, errors.New("string replacer: replacement string cannot be empty")
	}
	return f, nil
}

// Filter replaces a column with a fixed string replacement
func (f replaceByColumnFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.LineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.Typer, f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.Typer, f.whereFalse) == true {
		return r, nil
	}

	// find the column number to replace if it has not been initialised
	colNo, err := r.colNo(f.Column)
	if err != nil {
		return r, fmt.Errorf("string replace: %w", err)
	}

	// replace the column contents
	r.Columns[colNo] = f.Replacement
	return r, nil
}

// FilterName returns the Typer information about the replaceByColumnFilter
func (f replaceByColumnFilter) FilterName() string {
	return f.Typer
}

// fileByColumnFilter reads the contents of file into the struct and
// uses this to replace the contents of the designated column
type fileByColumnFilter struct {
	Typer        string
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
		Typer:      "file replace",
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
			return f, errors.New("file replacer: source contains a tab")
		}
		f.Replacements = append(f.Replacements, t)
	}
	// return an error if the scanner failed
	if err := scanner.Err(); err != nil {
		return f, err
	}

	return f, nil
}

// Filter replaces a column with the replacement indexed by the provided
// row number from the list of replacements. If the list of replacements
// has been exhausted, start from the top again
func (f fileByColumnFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.LineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.Typer, f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.Typer, f.whereFalse) == true {
		return r, nil
	}

	// find the column number to replace if it has not been initialised
	colNo, err := r.colNo(f.Column)
	if err != nil {
		return r, fmt.Errorf("file replacer: %w", err)
	}

	// replace the column contents with the replacement equalling the (1
	// indexed) row number of the input with the modulo of the length of
	// the replacements
	r.Columns[colNo] = f.Replacements[(r.LineNo-1)%len(f.Replacements)]
	return r, nil
}

// FilterName returns the Typer information about the fileByColumnFilter
func (f fileByColumnFilter) FilterName() string {
	return f.Typer
}

// UUIDFilter replaces the column with the output of a UUID
// generation function
type UUIDFilter struct {
	Typer      string
	Columns    []string
	whereTrue  map[string]string
	whereFalse map[string]string
}

// NewUUIDFilter makes a new UUIDFilter
func NewUUIDFilter(columns []string, whereTrue, whereFalse map[string]string) (*UUIDFilter, error) {

	f := UUIDFilter{
		Typer:      "uuid replace",
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
func (f UUIDFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.LineNo == 0 {
		return r, nil
	}

	// if no match for whereTrue conditions, return
	if len(f.whereTrue) > 0 && r.match(f.Typer, f.whereTrue) != true {
		return r, nil
	}
	// if match for whereFalse conditions, return
	if len(f.whereFalse) > 0 && r.match(f.Typer, f.whereFalse) == true {
		return r, nil
	}

	// find the column number to replace between the row r and filter f
	changed := 0
	for i, rc := range r.ColumnNames {
		for _, cn := range f.Columns {
			if rc == cn {
				changed++
				r.Columns[i] = uuid.New().String()
				break
			}
		}
	}

	if changed != len(f.Columns) {
		return r, errors.New("uuid replacer: could not find all column in UUIDFilter")
	}

	return r, nil
}

// FilterName returns the name of the filter type
func (f UUIDFilter) FilterName() string {
	return f.Typer
}

// ReplaceFilter allows multiple columns to be replaced by fixed
// strings, described by a slice of replacements. ReplaceFilter uses
// replaceByColumnFilter to do its work
type ReplaceFilter struct {
	Typer        string
	Columns      []string
	Replacements []string
	filters      []RowFilterer
}

// NewReplaceFilter creates a new ReplaceFilter, registering one or more
// replaceByColumnFilter filters to do the work of replacing each
// column.
func NewReplaceFilter(columns, replacements []string, whereTrue, whereFalse map[string]string) (*ReplaceFilter, error) {
	f := &ReplaceFilter{
		Typer:        "multi string replace",
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
	if r.LineNo == 0 {
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

// FilterName returns the name of the filter type
func (f ReplaceFilter) FilterName() string {
	return f.Typer
}

// FileFilter replaces a number of columns in a table with replacements
// from a tab delmited file (typically a postgres dump file). The actual
// work of this filter is performed by a set of fileByColumnFilter
// filter
type FileFilter struct {
	Typer        string
	Columns      []string
	Replacements []bytes.Buffer
	filters      []RowFilterer
}

// NewFileFilter creates a new FileFilter
func NewFileFilter(columns []string, fh io.Reader, whereTrue, whereFalse map[string]string) (*FileFilter, error) {
	f := &FileFilter{
		Typer:   "multi file replace",
		Columns: columns,
		filters: []RowFilterer{},
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
	if r.LineNo == 0 {
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

// FilterName returns the name of the filter type
func (f FileFilter) FilterName() string {
	return f.Typer
}
