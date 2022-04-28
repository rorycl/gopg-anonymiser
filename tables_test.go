package main

import (
	"bufio"
	"os"
	"strconv"
	"testing"
)

// mock filter implements RowFilterer
type mockFilter struct{}

// FilterName returns a mock filter name
func (f mockFilter) FilterName() string {
	return "mock filter"
}

// setRefDumpTable is an empty implementation
func (f *mockFilter) setRefDumpTable(rt RefTableRegister) {
	return
}

// getRefDumpTable is an empty implementation
func (f *mockFilter) getRefDumpTable() string {
	return ""
}

// Filter returns the provided row unchanged
func (f mockFilter) Filter(r Row) (Row, error) {
	return r, nil
}

func TestTable(t *testing.T) {

	f, err := os.Open("testdata/pg_dump.sql")
	if err != nil {
		t.Errorf("could not open test file")
	}
	defer f.Close()

	tf := tableFilters{
		tableFilters: map[string][]RowFilterer{
			"example_schema.events": []RowFilterer{
				&mockFilter{},
			},
		},
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		dt, err := NewDumpTable(scanner.Text(), false, tf)
		if err == ErrNoDumpTable {
			continue
		}
		if err == ErrNotInterestingTable {
			if dt.TableName != "public.users" && dt.TableName != "public.fkexample" {
				t.Errorf("not interesting table %s not public.users or public.fkexample", dt.TableName)
			}
			t.Logf("not interesting table %+v", dt)
			continue
		}
		if dt.TableName != "example_schema.events" {
			t.Errorf("expected example_schema.events, not %s", dt.TableName)
		}
		t.Logf("found table %+v", dt)
	}
}

func TestTableUsers(t *testing.T) {

	var err error

	f, err := os.Open("testdata/pg_dump.sql")
	if err != nil {
		t.Errorf("could not open test file")
	}
	defer f.Close()

	tf := tableFilters{
		tableFilters: map[string][]RowFilterer{
			"public.users": []RowFilterer{
				&mockFilter{},
			},
		},
	}

	scanner := bufio.NewScanner(f)
	lines := [][]string{}

	dt := new(DumpTable)
	for scanner.Scan() {

		if !dt.Inited() {
			dt, err = NewDumpTable(scanner.Text(), false, tf)
			if err == ErrNoDumpTable {
				continue
			}
			if err == ErrNotInterestingTable {
				continue
			}
			if dt.TableName != "public.users" {
				t.Errorf("expected public.users, not %s", dt.TableName)
			}
		} else {
			columns, ok := dt.LineSplitter(scanner.Text())
			if !ok {
				dt = new(DumpTable)
				continue
			}
			lines = append(lines, columns)
		}
	}

	if len(lines) != 6 {
		t.Error("6 lines not extracted from public.users")
	}

	for i, l := range lines {
		if l[0] != strconv.Itoa(i+1) {
			t.Errorf("line no %d incorrect for %v", i, l)
		}
		t.Logf("line extracted %d, %v", i, l)
	}
}

func TestRefTable(t *testing.T) {

	tf := tableFilters{
		tableFilters: map[string][]RowFilterer{
			"public.users": []RowFilterer{
				&mockFilter{},
			},
		},
	}

	line := `COPY public.users (name, age, password) FROM stdin;`

	dt, err := NewDumpTable(line, false, tf)
	if err != nil {
		t.Errorf("could not make new dump table %s", err)
	}
	rdt := &ReferenceDumpTable{
		DumpTable: dt,
	}

	origRows := []Row{
		Row{
			DumpTabler: dt,
			Columns:    []string{"Adam Applebaum", "20", "zut alors"},
			lineNo:     1,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Jenny Johnstone", "22", "password1"},
			lineNo:     2,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Zachary Zebb", "55", "qwerty yuiop"},
			lineNo:     3,
		},
	}

	newRows := []Row{
		Row{
			DumpTabler: dt,
			Columns:    []string{"Alice Applebaum", "20", "zut alors"},
			lineNo:     1,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Jeremy Johnstone", "22", "password1"},
			lineNo:     2,
		},
		Row{
			DumpTabler: dt,
			Columns:    []string{"Zena Zebb", "55", "qwerty yuiop"},
			lineNo:     3,
		},
	}

	for i, r := range origRows {
		rdt.addRow(true, r)
		rdt.addRow(false, newRows[i])
	}
	if len(rdt.originalRows) != len(rdt.latestRows) {
		t.Errorf("original rows len %d != latest rows len %d",
			len(rdt.originalRows), len(rdt.latestRows),
		)
	}

	gufv, err := rdt.getUpdatedFieldValue("age", "22", "name")
	if gufv != "Jeremy Johnstone" {
		t.Errorf("lookup for %s failed; got %s", "Jeremy Johnstone", gufv)
	}
}
