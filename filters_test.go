package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

var rows []Row

func init() {
	rows = []Row{
		Row{
			TableName:   "test",
			Columns:     []string{"Adam Applebaum", "20", "zut alors", "f86f06f8-bc48-11ec-9d40-07b727bf6764"},
			ColumnNames: []string{"name", "age", "password", "uuid"},
			LineNo:      1,
		},
		Row{
			TableName:   "test",
			Columns:     []string{"Jenny Johnstone", "22", "password1", "02613ac8-bc49-11ec-8037-3bad8c65b96e"},
			ColumnNames: []string{"name", "age", "password", "uuid"},
			LineNo:      2,
		},
		Row{
			TableName:   "test",
			Columns:     []string{"Zachary Zebb", "55", "qwerty yuiop", "09cf3bd4-bc49-11ec-83d6-ab2e063c8ce1"},
			ColumnNames: []string{"name", "age", "password", "uuid"},
			LineNo:      3,
		},
	}
}

func _filterNameTest(f RowFilterer, expected string) error {
	if expected != f.FilterName() {
		return fmt.Errorf("filter name %s != %s", expected, f.FilterName())
	}
	return nil
}

func TestDeleteFilter(t *testing.T) {

	filter, _ := NewDeleteFilter()

	if err := _filterNameTest(filter, "delete"); err != nil {
		t.Error(err)
	}

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

	_, err := NewreplaceByColumnFilter(
		"",
		"APassword",
	)
	if err == nil {
		t.Error("NewreplaceByColumnFilter init should fail")
	}

}

func TestStringReplaceFilter(t *testing.T) {

	filter, err := NewreplaceByColumnFilter(
		"password",
		"APassword",
	)
	if err != nil {
		t.Error("TestStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "string replace"); err != nil {
		t.Error(err)
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
	_, err := NewfileByColumnFilter(
		"",
		reader,
	)
	if err == nil {
		t.Error("fileByColumnFilter init should fail")
	}
	t.Log(err)
}

func TestFileReplaceFilterTabFail(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace\t2")
	_, err := NewfileByColumnFilter(
		"name",
		reader,
	)
	if err == nil {
		t.Error("fileByColumnFilter init should fail with tab fail")
	}
	t.Log(err)
}

func TestFileReplaceFilter(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	filter, err := NewfileByColumnFilter(
		"name",
		reader,
	)
	if err != nil {
		t.Error("TestFileReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "file replace"); err != nil {
		t.Error(err)
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

func TestUUIDReplaceFilter(t *testing.T) {

	filter, err := NewUUIDFilter("uuid")
	if err != nil {
		t.Errorf("Could not initialise uuid filter: %s", err)
	}
	for _, r := range rows {
		uuidOld := r.Columns[3]
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.LineNo, err)
		}
		if uuidOld == ro.Columns[3] {
			t.Errorf("Old uuid == new %s", uuidOld)
		}
		t.Logf("%+v\n", ro)
	}

}

// TestAllBasicFilters chains all filters other than the delete filter
func TestAllBasicFilters(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")

	filter, err := NewreplaceByColumnFilter(
		"password",
		"APassword",
	)
	if err != nil {
		t.Error("unexpected filter error")
	}

	filter2, err := NewfileByColumnFilter(
		"name",
		reader,
	)
	if err != nil {
		t.Error("unexpected filter2 error")
	}

	filter3, err := NewreplaceByColumnFilter(
		"age",
		"17.5",
	)

	filter4, err := NewUUIDFilter("uuid")
	if err != nil {
		t.Error("unexpected filter4 error")
	}

	expected := [][]string{
		[]string{"replace1", "17.5", "APassword"},
		[]string{"replace2", "17.5", "APassword"},
		[]string{"replace1", "17.5", "APassword"},
	}

	filterNames := []string{
		"string replace",
		"file replace",
		"string replace",
		"uuid replace",
	}

	for _, r := range rows {
		ro := r
		// use interface
		for i, f := range []RowFilterer{filter, filter2, filter3, filter4} {

			// test name
			if err := _filterNameTest(f, filterNames[i]); err != nil {
				t.Error(err)
			}

			// run filter
			ro, err = f.Filter(ro)
			if err != nil {
				t.Errorf(
					"Error filter %s at row linenumber %d: %v\n",
					f.FilterName(), r.LineNo, err,
				)
			}
		}
		for i, c := range ro.Columns {
			// need to skip uuid test
			if ro.ColumnNames[i] != "uuid" {
				if c != expected[r.LineNo-1][i] {
					t.Errorf("%v != %v", c, expected[r.LineNo-1][i])
				}
			} else {
				_, err := uuid.Parse(c)
				if err != nil {
					t.Errorf("not a valid uuid %v", c)
				}
			}
		}
		t.Logf("%+v\n", ro)
	}
}

func TestMultiStringReplaceFilter(t *testing.T) {

	filter, err := NewReplaceFilter(
		[]string{"password", "name"},
		[]string{"new password", "Carol Carnute"},
	)
	if err != nil {
		t.Error("TestMultiStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "multi string replace"); err != nil {
		t.Error(err)
	}

	var rowsCopy []Row
	copy(rows, rowsCopy)
	for _, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.LineNo, err)
		}

		if !(ro.Columns[0] == "Carol Carnute" && ro.Columns[2] == "new password") {
			t.Errorf("replacements did not work : %s %s", ro.Columns[0], ro.Columns[2])
		}
		t.Logf("%+v\n", ro.Columns)
	}
}

// TestMultiFileReplaceFilterFail has fewer columns than the reader
func TestMultiFileReplaceFilterFail(t *testing.T) {

	reader := strings.NewReader(`John James	29	xyz1356
Brady Brighton	30	caz1357
Norris Naughton	31	dba2468`)

	_, err := NewFileFilter(
		[]string{"name", "age"},
		reader,
	)
	if err == nil {
		t.Errorf("TestMultiFileReplaceFilter should fail with too many file columns")
	}
	t.Log(err)
}

func TestMultiFileReplaceFilter(t *testing.T) {

	reader := strings.NewReader(`John James	29	xyz1356
Brady Brighton	30	caz1357
Norris Naughton	31	dba2468`)

	filter, err := NewFileFilter(
		[]string{"name", "age", "password"},
		reader,
	)
	if err != nil {
		t.Errorf("TestMultiFileReplaceFilter failed init: %s", err)
	}

	if err := _filterNameTest(filter, "multi file replace"); err != nil {
		t.Error(err)
	}

	expected := [][]string{
		[]string{"John James", "29", "xyz1356", "f86f06f8-bc48-11ec-9d40-07b727bf6764"},
		[]string{"Brady Brighton", "30", "caz1357", "02613ac8-bc49-11ec-8037-3bad8c65b96e"},
		[]string{"Norris Naughton", "31", "dba2468", "09cf3bd4-bc49-11ec-83d6-ab2e063c8ce1"},
	}

	var rowsCopy []Row
	copy(rows, rowsCopy)
	for i, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Multifile error on row linenumber %d: %v\n", r.LineNo, err)
		}
		for ii, c := range ro.Columns {
			if c != expected[i][ii] {
				t.Errorf("replacement expected %s, got %s", expected[i][ii], c)
			}
		}
		t.Log(ro.Columns)
	}
}
