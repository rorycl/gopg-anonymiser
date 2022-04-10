package main

import (
	"testing"
)

var rows []Row

func init() {
	rows = []Row{
		Row{
			TableName:   "test",
			Columns:     []string{"Adam Applebaum", "20", "zut alors"},
			ColumnNames: []string{"name", "age", "password"},
			LineNo:      1,
		},
		Row{
			TableName:   "test",
			Columns:     []string{"Jenny Johnstone", "22", "password1"},
			ColumnNames: []string{"name", "age", "password"},
			LineNo:      2,
		},
		Row{
			TableName:   "test",
			Columns:     []string{"Zachary Zebb", "55", "qwerty yuiop"},
			ColumnNames: []string{"name", "age", "password"},
			LineNo:      3,
		},
	}
}

func TestStringReplaceFilter(t *testing.T) {

	filter := RowStringReplaceFilter{
		Column:      "password",
		Replacement: "APassword",
	}

	for _, r := range rows {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.LineNo, err)
		}
		if ro.Columns[2] != filter.Replacement {
			t.Errorf("Column 2 on Line %d with val %s != %s",
				r.LineNo, ro.Columns[2], filter.Replacement,
			)
		}
		t.Logf("%+v\n", ro)
	}
}
