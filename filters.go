package main

// Row holds a line (represented by columnar data) from a postgresql
// dump file describing the contents of a postgreql table, together with
// the name of table, the column names and the line number (excluding
// header) within the table using a 1-indexed count
type Row struct {
	TableName   string
	Columns     []string
	ColumnNames []string
	LineNo      integer
}

// RowFilterer is the interface that any row filter needs to fulfil to
// filter a row, perhaps on a column basis, and allow chaining of
type RowFilterer interface {
	Filter(r Row, args ...string) (Row, error)
}

// RowDeleteFilter removes all lines
type RowDeleteFilter struct {
}

// Filter returns an empty row
func (f RowDeleteFilter) Filter(r Row, args ...string) (Row, error) {
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
func (f RowStringReplaceFilter) Filter(r Row, args ...string) (Row, error) {
	// if
	if Row.LineNo == 0 {
		return r, nil
	}
	colNo := -1
	for c := 0; c < len(r.Columns); c++ {
		if r.ColumnNames[c] == f.Column {
			colNo = c
			break
		}
	}
	if colNo == -1 {
		return r, fmt.Errorsf("Could not find column %s in RowStringReplaceFilter", f.Column)
	}
	r.Columns[colNo] = f.Replacement
	return r, nil

}
