package main

import (
	"strings"
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

func TestRowDeleteFilter(t *testing.T) {

	filter, _ := NewRowDeleteFilter()

	for i, r := range rows {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", i, err)
		}
		if ro.Columns != nil {
			t.Error("columns should be an empty struct")
		}
		t.Logf("%+v\n", ro)
	}
}

func TestStringReplaceFilterFail(t *testing.T) {

	_, err := NewRowStringReplaceFilter(
		"",
		"APassword",
	)
	if err == nil {
		t.Error("NewRowStringReplaceFilter init should faile")
	}

}

func TestStringReplaceFilter(t *testing.T) {

	filter, err := NewRowStringReplaceFilter(
		"password",
		"APassword",
	)

	if err != nil {
		t.Error("TestStringReplaceFilter failed init")
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

func TestFileReplaceFilterFail(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	_, err := NewRowFileReplaceFilter(
		"",
		reader,
	)
	if err == nil {
		t.Error("RowFileReplaceFilter init should fail")
	}
	t.Log(err)
}

func TestFileReplaceFilterCommaFail(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace,2")
	_, err := NewRowFileReplaceFilter(
		"name",
		reader,
	)
	if err == nil {
		t.Error("RowFileReplaceFilter init should fail with comma fail")
	}
	t.Log(err)
}

func TestFileReplaceFilter(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	filter, err := NewRowFileReplaceFilter(
		"name",
		reader,
	)

	if err != nil {
		t.Error("TestFileReplaceFilter failed init")
	}

	replacements := map[int]string{
		1: "replace1",
		2: "replace2",
		3: "replace1",
	}

	for _, r := range rows {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.LineNo, err)
		}
		if ro.Columns[0] != replacements[r.LineNo] {
			t.Errorf("Column 0 on Line %d with val %s != %s",
				r.LineNo, ro.Columns[0], replacements[r.LineNo],
			)
		}
		t.Logf("%+v\n", ro)
	}
}

// TestAllFilters chains all filters other than the delete filter
func TestAllFilters(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")

	filter, err := NewRowStringReplaceFilter(
		"password",
		"APassword",
	)
	if err != nil {
		t.Error("unexpected filter error")
	}

	filter2, err := NewRowFileReplaceFilter(
		"name",
		reader,
	)
	if err != nil {
		t.Error("unexpected filter2 error")
	}

	filter3, err := NewRowStringReplaceFilter(
		"age",
		"17.5",
	)
	if err != nil {
		t.Error("unexpected filter3 error")
	}

	expected := [][]string{
		[]string{"replace1", "17.5", "APassword"},
		[]string{"replace2", "17.5", "APassword"},
		[]string{"replace1", "17.5", "APassword"},
	}

	for _, r := range rows {
		ro := r
		// use interface
		for _, f := range []RowFilterer{filter, filter2, filter3} {
			ro, err = f.Filter(ro)
			if err != nil {
				t.Errorf(
					"Error filter %s at row linenumber %d: %v\n",
					f.TypeName(), r.LineNo, err,
				)
			}
		}
		for i, c := range ro.Columns {
			if c != expected[r.LineNo-1][i] {
				t.Errorf("%v != %v", c, expected[r.LineNo-1][i])
			}
		}
		t.Logf("%+v\n", ro)
	}
}
