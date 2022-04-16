package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
)

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

// RowFilterer is the interface that any row filter needs to fulfil to
// filter a row, perhaps on a column basis, and allow chaining of
type RowFilterer interface {
	// FilterName returns the name of the filter
	FilterName() string
	// Filter performs filtering on a row of data
	Filter(r Row) (Row, error)
}

// RowDeleteFilter removes all lines
type RowDeleteFilter struct {
	Typer string
}

// NewRowDeleteFilter makes a new RowDeleteFilter
func NewRowDeleteFilter() (*RowDeleteFilter, error) {
	return &RowDeleteFilter{"delete"}, nil
}

// Filter returns an empty row
func (f RowDeleteFilter) Filter(r Row) (Row, error) {
	var rr Row
	return rr, nil
}

// FilterName returns the Typer information about the RowDeleteFilter
func (f RowDeleteFilter) FilterName() string {
	return f.Typer
}

// RowStringReplaceFilter replaces a column named "Column" with the
// provided replacement string
type RowStringReplaceFilter struct {
	Typer       string
	Column      string
	Replacement string
	colNo       int
}

// NewRowStringReplaceFilter makes a new RowStringReplaceFilter
func NewRowStringReplaceFilter(column, replacement string) (*RowStringReplaceFilter, error) {
	r := &RowStringReplaceFilter{
		Typer:       "string replace",
		Column:      column,
		Replacement: replacement,
		colNo:       -1,
	}
	if column == "" {
		return r, errors.New("string replacer: column name cannot be empty")
	}
	if replacement == "" {
		return r, errors.New("string replacer: replacement string cannot be empty")
	}
	return r, nil
}

// Filter replaces a column with a fixed string replacement
func (f RowStringReplaceFilter) Filter(r Row) (Row, error) {
	// if there is no line number the previous filter may have stopped
	// processing
	if r.LineNo == 0 {
		return r, nil
	}

	// find the column number to replace if it has not been initialised
	if f.colNo == -1 {
		for c := 0; c < len(r.Columns); c++ {
			if r.ColumnNames[c] == f.Column {
				f.colNo = c
				break
			}
		}
		if f.colNo == -1 {
			return r, fmt.Errorf(
				"string replacer: could not find column %s in RowStringReplaceFilter", f.Column,
			)
		}
	}

	// replace the column contents
	r.Columns[f.colNo] = f.Replacement
	return r, nil

}

// FilterName returns the Typer information about the RowStringReplaceFilter
func (f RowStringReplaceFilter) FilterName() string {
	return f.Typer
}

// RowFileReplaceFilter reads the contents of file into the struct and
// uses this to replace the contents of the designated column
type RowFileReplaceFilter struct {
	Typer        string
	Column       string
	Replacements []string
}

// NewRowFileReplaceFilter makes a new RowFileReplaceFilter
func NewRowFileReplaceFilter(column string, f io.Reader) (*RowFileReplaceFilter, error) {

	r := &RowFileReplaceFilter{
		Typer:  "file replace",
		Column: column,
	}
	if column == "" {
		return r, errors.New("file replacer: column name cannot be empty")
	}

	// append the replacement lines to the replacement slice
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		t := scanner.Text()
		if strings.Contains(t, "\t") {
			return r, errors.New("file replacer: source contains a tab")
		}
		r.Replacements = append(r.Replacements, t)
	}
	// return an error if the scanner failed
	if err := scanner.Err(); err != nil {
		return r, err
	}

	return r, nil
}

// Filter replaces a column with the replacement indexed by the provided
// row number from the list of replacements. If the list of replacements
// has been exhausted, start from the top again
func (f RowFileReplaceFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.LineNo == 0 {
		return r, nil
	}

	// find the column number to replace
	colNo := -1
	for c := 0; c < len(r.Columns); c++ {
		if r.ColumnNames[c] == f.Column {
			colNo = c
			break
		}
	}
	if colNo == -1 {
		return r, fmt.Errorf(
			"file replacer: could not find column %s in RowFileReplaceFilter", f.Column,
		)
	}

	// replace the column contents with the replacement equalling the (1
	// indexed) row number of the input with the modulo of the length of
	// the replacements
	r.Columns[colNo] = f.Replacements[(r.LineNo-1)%len(f.Replacements)]
	return r, nil

}

// FilterName returns the Typer information about the RowFileReplaceFilter
func (f RowFileReplaceFilter) FilterName() string {
	return f.Typer
}

// RowFilterUUIDFilter replaces the column with the output of a UUID
// generation function
type RowFilterUUIDFilter struct {
	Typer  string
	Column string
}

// NewRowFilterUUIDFilter makes a new RowFilterUUIDFilter
func NewRowFilterUUIDFilter(column string) (*RowFilterUUIDFilter, error) {
	return &RowFilterUUIDFilter{
		Typer:  "uuid replace",
		Column: column,
	}, nil
}

// Filter replaces a column with the replacement indexed by the provided
// row number with a uuid
func (f RowFilterUUIDFilter) Filter(r Row) (Row, error) {

	// if there is no line number the previous filter may have stopped
	// processing
	if r.LineNo == 0 {
		return r, nil
	}

	// find the column number to replace
	colNo := -1
	for c := 0; c < len(r.Columns); c++ {
		if r.ColumnNames[c] == f.Column {
			colNo = c
			break
		}
	}
	if colNo == -1 {
		return r, fmt.Errorf(
			"uuid replacer: could not find column %s in RowFilterUUIDFilter", f.Column,
		)
	}
	u := uuid.New()
	r.Columns[colNo] = u.String()
	return r, nil
}

// FilterName returns the name of the filter type
func (f RowFilterUUIDFilter) FilterName() string {
	return f.Typer
}

// RowMultiStringReplaceFilter allows multiple columns to be replaced by
// fixed strings, described by a slice of replacements.
// RowMultiStringReplaceFilter uses RowStringReplaceFilter to do its
// work
type RowMultiStringReplaceFilter struct {
	Typer        string
	Columns      []string
	Replacements []string
	filters      []RowFilterer
}

// NewRowMultiStringReplaceFilter creates a new
// RowMultiStringReplaceFilter, registering one or more
// RowStringReplaceFilter filters to do the work of replacing each
// column.
func NewRowMultiStringReplaceFilter(columns, replacements []string) (*RowMultiStringReplaceFilter, error) {
	r := &RowMultiStringReplaceFilter{
		Typer:        "multi string replace",
		Columns:      columns,
		Replacements: replacements,
		filters:      []RowFilterer{},
	}
	if len(columns) == 0 {
		return r, errors.New("multi string replace: at least one column must be specified")
	}
	if len(columns) != len(replacements) {
		return r, errors.New("multie string replace: number of columns and replacements must be the same")
	}
	for i, c := range r.Columns {
		f, err := NewRowStringReplaceFilter(c, r.Replacements[i])
		if err != nil {
			return r, fmt.Errorf("multi string init error %w", err)
		}
		r.filters = append(r.filters, f)
	}
	return r, nil
}

// Filter replaces column values with a fixed string replacement using
// one or more RowStringReplaceFilter filters
func (f RowMultiStringReplaceFilter) Filter(r Row) (Row, error) {
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
func (f RowMultiStringReplaceFilter) FilterName() string {
	return f.Typer
}
