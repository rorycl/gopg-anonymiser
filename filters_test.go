package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

var rows []Row
var rdt *ReferenceDumpTable
var rtr = make(RefTableRegister)

func init() {
	dt := &DumpTable{
		TableName:   "test",
		columnNames: []string{"name", "age", "password", "uuid"},
		initialised: true,
	}
	rows = []Row{
		Row{
			DumpTabler: dt,
			Columns:    []string{"Adam Applebaum", "20", "zut alors", "f86f06f8-bc48-11ec-9d40-07b727bf6764"},
			lineNo:     1,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Jenny Johnstone", "22", "password1", "02613ac8-bc49-11ec-8037-3bad8c65b96e"},
			lineNo:     2,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Zachary Zebb", "55", "qwerty yuiop", "09cf3bd4-bc49-11ec-83d6-ab2e063c8ce1"},
			lineNo:     3,
		},
	}
	// reference dump table
	rdt = &ReferenceDumpTable{
		DumpTable: &DumpTable{
			TableName:   "public.users",
			columnNames: []string{"name", "age", "password", "uuid"},
			initialised: true,
		},
	}

	rdt.originalRows = []Row{
		Row{
			DumpTabler: rdt,
			Columns:    []string{"Adam Applebaum", "20", "zut alors", "f86f06f8-bc48-11ec-9d40-07b727bf6764"},
			lineNo:     1,
		},
		Row{
			DumpTabler: rdt,
			Columns:    []string{"Jenny Johnstone", "22", "password1", "02613ac8-bc49-11ec-8037-3bad8c65b96e"},
			lineNo:     2,
		},
		Row{
			DumpTabler: rdt,
			Columns:    []string{"Zachary Zebb", "55", "qwerty yuiop", "09cf3bd4-bc49-11ec-83d6-ab2e063c8ce1"},
			lineNo:     3,
		},
	}

	rdt.latestRows = []Row{
		Row{
			DumpTabler: rdt,
			Columns:    []string{"Alice Applebaum", "20", "zut alors", "f86f06f8-bc48-11ec-9d40-07b727bf6764"},
			lineNo:     1,
		},
		Row{
			DumpTabler: rdt,
			Columns:    []string{"Jonas Savimbi", "22", "password1", "02613ac8-bc49-11ec-8037-3bad8c65b96e"},
			lineNo:     2,
		},
		Row{
			DumpTabler: rdt,
			Columns:    []string{"Zora Zucchini", "55", "qwerty yuiop", "09cf3bd4-bc49-11ec-83d6-ab2e063c8ce1"},
			lineNo:     3,
		},
	}

	rtr["public.users"] = rdt
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

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
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

	_, err := newReplaceByColumnFilter(
		"",
		"APassword",
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err == nil {
		t.Error("newReplaceByColumnFilter init should fail")
	}

}

func TestStringReplaceFilter(t *testing.T) {

	filter, err := newReplaceByColumnFilter(
		"password",
		"APassword",
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err != nil {
		t.Error("TestStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "string replace"); err != nil {
		t.Error(err)
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		if ro.Columns[2] != filter.Replacement {
			t.Errorf("Column 2 on Line %d with val %s != %s",
				r.lineNo, ro.Columns[2], filter.Replacement,
			)
		}
		t.Logf("%+v\n", ro)
	}
}

func TestStringReplaceFilterWhereTrue(t *testing.T) {

	filter, err := newReplaceByColumnFilter(
		"password",
		"APassword",
		map[string]string{"name": "Adam Applebaum"}, // whereTrue
		map[string]string{},                         // whereFalse
	)
	if err != nil {
		t.Error("TestStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "string replace"); err != nil {
		t.Error(err)
	}

	expected := []struct {
		name     string
		password string
	}{
		{name: "Adam Applebaum", password: "APassword"},
		{name: "Jenny Johnstone", password: "password1"},
		{name: "Zachary Zebb", password: "qwerty yuiop"},
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		rowPassword, err := ro.colVal("password")
		if err != nil {
			t.Errorf("Unexpected colVal error: %w\n", err)
		}

		if rowPassword != expected[i].password {
			t.Errorf(
				"name %s got %s want %s",
				expected[i].name, rowPassword, expected[i].password,
			)
		}
		t.Logf("%+v\n", ro)
	}
}

func TestStringReplaceFilterWhereFalse(t *testing.T) {

	filter, err := newReplaceByColumnFilter(
		"password",
		"APassword",
		map[string]string{}, // whereTrue
		map[string]string{"name": "Adam Applebaum"}, // whereFalse
	)
	if err != nil {
		t.Error("TestStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "string replace"); err != nil {
		t.Error(err)
	}

	expected := []struct {
		name     string
		password string
	}{
		{name: "Adam Applebaum", password: "zut alors"},
		{name: "Jenny Johnstone", password: "APassword"},
		{name: "Zachary Zebb", password: "APassword"},
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		rowPassword, err := ro.colVal("password")
		if err != nil {
			t.Errorf("Unexpected colVal error: %w\n", err)
		}

		if rowPassword != expected[i].password {
			t.Errorf(
				"name %s got %s want %s",
				expected[i].name, rowPassword, expected[i].password,
			)
		}
		t.Logf("%+v\n", ro)
	}
}

// Test Nulls
func TestStringReplaceFilterWhereTrueNULL(t *testing.T) {

	dt := &DumpTable{
		TableName:   "test",
		columnNames: []string{"name", "age", "password", "uuid"},
		initialised: true,
	}
	rows = []Row{
		Row{
			DumpTabler: dt,
			Columns:    []string{"Adam Applebaum", "20", `\N`, "f86f06f8-bc48-11ec-9d40-07b727bf6764"},
			lineNo:     1,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Jenny Johnstone", "22", "password1", "02613ac8-bc49-11ec-8037-3bad8c65b96e"},
			lineNo:     2,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Zachary Zebb", "55", "\\N", "09cf3bd4-bc49-11ec-83d6-ab2e063c8ce1"},
			lineNo:     3,
		},
	}

	filter, err := newReplaceByColumnFilter(
		"password",
		"APassword",
		map[string]string{"password": "\\N"}, // whereTrue
		map[string]string{},                  // whereFalse
	)
	if err != nil {
		t.Error("TestStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "string replace"); err != nil {
		t.Error(err)
	}

	expected := []struct {
		name     string
		password string
	}{
		{name: "Adam Applebaum", password: "APassword"},
		{name: "Jenny Johnstone", password: "password1"},
		{name: "Zachary Zebb", password: "APassword"},
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		rowPassword, err := ro.colVal("password")
		if err != nil {
			t.Errorf("Unexpected colVal error: %w\n", err)
		}

		if rowPassword != expected[i].password {
			t.Errorf(
				"name %s got %s want %s",
				expected[i].name, rowPassword, expected[i].password,
			)
		}
		t.Logf("%+v\n", ro)
	}
}

func TestFileReplaceFilterFail(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	_, err := newFileByColumnFilter(
		"",
		reader,
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err == nil {
		t.Error("fileByColumnFilter init should fail")
	}
	t.Log(err)
}

func TestFileReplaceFilterTabFail(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace\t2")
	_, err := newFileByColumnFilter(
		"name",
		reader,
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err == nil {
		t.Error("fileByColumnFilter init should fail with tab fail")
	}
	t.Log(err)
}

func TestFileReplaceFilter(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	filter, err := newFileByColumnFilter(
		"name",
		reader,
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
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

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		if ro.Columns[0] != replacements[r.lineNo] {
			t.Errorf("Column 0 on Line %d with val %s != %s",
				r.lineNo, ro.Columns[0], replacements[r.lineNo],
			)
		}
		t.Logf("%+v\n", ro)
	}
}

func TestFileReplaceFilterWhereTrue(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	filter, err := newFileByColumnFilter(
		"name",
		reader,
		map[string]string{"age": "20"}, // whereTrue
		map[string]string{},            // whereFalse
	)
	if err != nil {
		t.Error("TestFileReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "file replace"); err != nil {
		t.Error(err)
	}

	replacements := map[int]string{
		1: "replace1",
		2: "Jenny Johnstone",
		3: "Zachary Zebb",
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		if ro.Columns[0] != replacements[r.lineNo] {
			t.Errorf("Column 0 on Line %d with val %s != %s",
				r.lineNo, ro.Columns[0], replacements[r.lineNo],
			)
		}
		t.Logf("%+v\n", ro)
	}
}

func TestFileReplaceFilterWhereFalse(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")
	filter, err := newFileByColumnFilter(
		"name",
		reader,
		map[string]string{},            // whereTrue
		map[string]string{"age": "20"}, // whereFalse
	)
	if err != nil {
		t.Error("TestFileReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "file replace"); err != nil {
		t.Error(err)
	}

	replacements := map[int]string{
		1: "Adam Applebaum",
		2: "replace2",
		3: "replace1",
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		if ro.Columns[0] != replacements[r.lineNo] {
			t.Errorf("Column 0 on Line %d with val %s != %s",
				r.lineNo, ro.Columns[0], replacements[r.lineNo],
			)
		}
		t.Logf("%+v\n", ro)
	}
}

func TestUUIDReplaceFilter(t *testing.T) {

	filter, err := NewUUIDFilter(
		[]string{"uuid"},
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)

	if err != nil {
		t.Errorf("Could not initialise uuid filter: %s", err)
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
		uuidOld := r.Columns[3]
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		if uuidOld == ro.Columns[3] {
			t.Errorf("Old uuid == new %s", uuidOld)
		}
		t.Logf("%+v\n", ro)
	}

}

func TestUUIDReplaceFilterWhereTrue(t *testing.T) {

	filter, err := NewUUIDFilter(
		[]string{"uuid"},
		map[string]string{"age": "55"}, // whereTrue
		map[string]string{},            // whereFalse
	)

	if err != nil {
		t.Errorf("Could not initialise uuid filter: %s", err)
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		uuidOld := r.Columns[3]
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		// third row should keep the same uuid
		if i == 2 {
			if uuidOld == ro.Columns[3] {
				t.Errorf("Row %d uuid old %s == new %s", i, uuidOld, ro.Columns[3])
			}
		} else if uuidOld != ro.Columns[3] {
			t.Errorf("** Row %d uuid old %s != new %s", i, uuidOld, ro.Columns[3])
		}
		t.Logf("%+v\n", ro)
	}

}

func TestUUIDReplaceFilterWhereFalse(t *testing.T) {

	filter, err := NewUUIDFilter(
		[]string{"uuid"},
		map[string]string{},            // whereTrue
		map[string]string{"age": "55"}, // whereFalse
	)

	if err != nil {
		t.Errorf("Could not initialise uuid filter: %s", err)
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		uuidOld := r.Columns[3]
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
		}
		// third row should keep the same uuid
		if i == 2 {
			if uuidOld != ro.Columns[3] {
				t.Errorf("Row %d uuid old %s != new %s", i, uuidOld, ro.Columns[3])
			}
		} else if uuidOld == ro.Columns[3] {
			t.Errorf("** Row %d uuid old %s == new %s", i, uuidOld, ro.Columns[3])
		}
		t.Logf("%+v\n", ro)
	}

}

// TestAllBasicFilters chains all filters other than the delete filter
func TestAllBasicFilters(t *testing.T) {

	reader := strings.NewReader("replace1\nreplace2")

	filter, err := newReplaceByColumnFilter(
		"password",
		"APassword",
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err != nil {
		t.Error("unexpected filter error")
	}

	filter2, err := newFileByColumnFilter(
		"name",
		reader,
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err != nil {
		t.Error("unexpected filter2 error")
	}

	filter3, err := newReplaceByColumnFilter(
		"age",
		"17.5",
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)

	filter4, err := NewUUIDFilter(
		[]string{"uuid"},
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
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

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
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
					f.FilterName(), r.lineNo, err,
				)
			}
		}
		for i, c := range ro.Columns {
			// need to skip uuid test
			if ro.ColumnNames()[i] != "uuid" {
				if c != expected[r.lineNo-1][i] {
					t.Errorf("%v != %v", c, expected[r.lineNo-1][i])
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
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
	)
	if err != nil {
		t.Error("TestMultiStringReplaceFilter failed init")
	}

	if err := _filterNameTest(filter, "multi string replace"); err != nil {
		t.Error(err)
	}

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for _, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Error on row linenumber %d: %v\n", r.lineNo, err)
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
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
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
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
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
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Multifile error on row linenumber %d: %v\n", r.lineNo, err)
		}
		for ii, c := range ro.Columns {
			if c != expected[i][ii] {
				t.Errorf("replacement expected %s, got %s", expected[i][ii], c)
			}
		}
		t.Log(ro.Columns)
	}
}

func TestNewReferenceFilter(t *testing.T) {

	filter, err := NewReferenceFilter(
		// replace the name column using age as a key to the foreign
		// table
		[]string{"name"},    // local col to replace
		[]string{"name"},    // remote value col
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
		"age",               // local key
		"public.users.age",  // remote key column
	)
	if err != nil {
		t.Errorf("could not register reference filter %w", err)
	}
	// register the referenced dump tables
	filter.setRefDumpTable(rtr)

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		fmt.Printf("in : %v\n", r.Columns)
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Reference error on row linenumber %d: %v\n", r.lineNo, err)
		}
		// name should change
		have, _ := ro.colVal("name")
		want, _ := rdt.latestRows[i].colVal("name")
		if have != want {
			t.Errorf("processed name %s != %s", have, want)
		}
		// password should not change
		have, _ = ro.colVal("password")
		want, _ = rdt.originalRows[i].colVal("password")
		if have != want {
			t.Errorf("processed password have %s want %s", have, want)
		}
		fmt.Printf("out : %v\n", r.Columns)
	}
}

func TestNewReferenceFilterWithAdd(t *testing.T) {

	filter, err := NewReferenceFilter(
		// replace the name column using age as a key to the foreign
		// table
		[]string{"name"},    // local col to replace
		[]string{"name"},    // remote value col
		map[string]string{}, // whereTrue
		map[string]string{}, // whereFalse
		"age",               // local key
		"public.users.age",  // remote key column
	)
	if err != nil {
		t.Errorf("could not register reference filter %w", err)
	}

	// grab, zero and reset rdt rows
	var oRows []Row
	copy(oRows, rdt.originalRows)
	rdt.originalRows = []Row{}
	var lRows []Row
	copy(lRows, rdt.latestRows)
	rdt.latestRows = []Row{}

	// add the rows back in
	for _, r := range oRows {
		rdt.addRow(true, r)
	}
	for _, r := range lRows {
		rdt.addRow(false, r)
	}

	// register the referenced dump tables
	filter.setRefDumpTable(rtr)

	var rowsCopy []Row
	copy(rowsCopy, rows)
	for i, r := range rowsCopy {
		fmt.Printf("in : %v\n", r.Columns)
		ro, err := filter.Filter(r)
		if err != nil {
			t.Errorf("Reference error on row linenumber %d: %v\n", r.lineNo, err)
		}
		// name should change
		have, _ := ro.colVal("name")
		want, _ := rdt.latestRows[i].colVal("name")
		if have != want {
			t.Errorf("processed name %s != %s", have, want)
		}
		// password should not change
		have, _ = ro.colVal("password")
		want, _ = rdt.originalRows[i].colVal("password")
		if have != want {
			t.Errorf("processed password have %s want %s", have, want)
		}
		fmt.Printf("out : %v\n", r.Columns)
	}
}
