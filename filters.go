package main

import (
	"fmt"
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
	Filter(r Row) (Row, error)
}

// RowDeleteFilter removes all lines
type RowDeleteFilter struct {
}

// Filter returns an empty row
func (f RowDeleteFilter) Filter(r Row) (Row, error) {
	var rr Row
	return rr, nil
}

// RowStringReplaceFilter replaces a column named "Column" with the
// provided replacement string
type RowStringReplaceFilter struct {
	Column      string
	Replacement string
}

// Filter replaces a column with a fixed string replacement
func (f RowStringReplaceFilter) Filter(r Row) (Row, error) {
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
			"Could not find column %s in RowStringReplaceFilter", f.Column,
		)
	}

	// replace the column contents
	r.Columns[colNo] = f.Replacement
	return r, nil

}
