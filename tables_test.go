package main

import (
	"bufio"
	"os"
	"strconv"
	"testing"
)

func TestTable(t *testing.T) {

	f, err := os.Open("testdata/pg_dump.sql")
	if err != nil {
		t.Errorf("could not open test file")
	}
	defer f.Close()

	interestingTables := []string{"example_schema.events"}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		dt, err := NewDumpTable(scanner.Text(), interestingTables)
		if err == ErrNoDumpTable {
			continue
		}
		if err == ErrNotInterestingTable {
			if dt.TableName != "public.users" {
				t.Errorf("not interesting table %s not public.users", dt.TableName)
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

	dt := new(DumpTable)
	interestingTables := []string{"public.users"}

	scanner := bufio.NewScanner(f)
	lines := [][]string{}

	for scanner.Scan() {

		if !dt.Inited() {
			dt, err = NewDumpTable(scanner.Text(), interestingTables)
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