package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// loadFilters loads a set of table transformation filters for a dump
// table from settings
func loadFilters(settings Settings, dt *DumpTable) ([]RowFilterer, error) {

	rfs := []RowFilterer{}

	// retrieve settings for this dump table else error
	var filters []Filter
	for tableName, tableFilters := range settings {
		if tableName == dt.TableName {
			filters = tableFilters
			break
		}
	}
	if len(filters) == 0 {
		return rfs, fmt.Errorf("table '%s' could not be found in settings", dt.TableName)
	}

	// load filters
	for _, f := range filters {

		switch f.Filter {
		case "delete":
			filter, _ := NewDeleteFilter()
			rfs = append(rfs, filter)

		case "uuid":
			filter, err := NewUUIDFilter(f.Columns, f.If, f.NotIf)
			// fixme
			if err != nil {
				return rfs, fmt.Errorf("uuid filter error: %w", err)
			}
			rfs = append(rfs, filter)

		case "string replace":
			if len(f.Columns) < 1 {
				return rfs, errors.New("string replace filter: must provide at lease one column")
			}
			if len(f.Columns) != len(f.Replacements) {
				return rfs, errors.New("string replace filter: column length != replacement length")
			}
			filter, err := NewReplaceFilter(
				f.Columns,
				f.Replacements,
				f.If,
				f.NotIf,
			)
			if err != nil {
				return rfs, fmt.Errorf("source error for string replace: %w", err)
			}
			rfs = append(rfs, filter)

		case "file replace":
			if len(f.Columns) < 1 {
				return rfs, errors.New("file replace: must provide at lease one column")
			}
			filer, err := os.Open(f.Source)
			if err != nil {
				return rfs, fmt.Errorf("file replace filter error: %w", err)
			}
			filter, err := NewFileFilter(
				f.Columns,
				filer,
				f.If,
				f.NotIf,
			)
			if err != nil {
				return rfs, fmt.Errorf("source error for file error: %w", err)
			}
			rfs = append(rfs, filter)

		default:
			return rfs, fmt.Errorf("filter type %s not known", f.Filter)
		}
	}
	return rfs, nil
}

// anonArgs is the Anonymise function signature
type anonArgs struct {
	// a postgresql dump file via either os.Stdin or a file
	dumpFile io.Reader
	// a toml settings file
	settingsFile string
	// output to either os.Stdout or a file
	output io.Writer
	// only show changed tables inthe output
	changedOnly bool
}

// Anonymise anonymises a postgresql dump file
func Anonymise(args anonArgs) error {

	// load settings
	settings, err := LoadToml(args.settingsFile)
	if err != nil {
		return fmt.Errorf("settings file load error %s", err)
	}

	interestingTables := []string{}
	for tableName := range settings {
		interestingTables = append(interestingTables, tableName)
	}

	// init a dump table and related filters
	dt := new(DumpTable)
	dtFilters := []RowFilterer{}

	scanner := bufio.NewScanner(args.dumpFile)

	var lineNo = 0
	for scanner.Scan() {

		t := scanner.Text()

		if !dt.Inited() {

			// try to init dt
			dt, err = NewDumpTable(t, interestingTables)
			switch err {
			case ErrNoDumpTable:
			case ErrNotInterestingTable:
			case nil:
			default:
				return fmt.Errorf("Error parsing line %s", t)
			}

			// load filters if dt inited
			if dt.Inited() {
				dtFilters, err = loadFilters(settings, dt)
				if err != nil {
					return fmt.Errorf("loading filters failed %w", err)
				}
				// re-initiliased in-dump line numbers
				lineNo = 0
			}

		} else {
			// the dump table is initialised; filter the lines unless
			// the end of table marker is found

			// count lines from 1
			lineNo++

			columns, ok := dt.LineSplitter(scanner.Text())
			if !ok {
				dt = new(DumpTable)
				if args.changedOnly {
					continue
				}
				_, err := io.WriteString(args.output, t+"\n")
				if err != nil {
					return fmt.Errorf("write error: %w", err)
				}
				continue
			}

			// make rows
			row := Row{
				TableName:   dt.TableName,
				Columns:     columns,
				ColumnNames: dt.ColumnNames,
				LineNo:      lineNo,
			}
			// filter rows
			for _, f := range dtFilters {
				row, err = f.Filter(row)
				if err != nil {
					return fmt.Errorf("filter error on table %s: %w", dt.TableName, err)
				}
			}
			// convert columns back to t unless the Row has been deleted
			if row.LineNo == 0 {
				t = "row deleted"
			} else {
				t = strings.Join(row.Columns, "\t")
			}
		}

		if t == "row deleted" {
			continue
		}

		// output
		if args.changedOnly && !dt.Inited() {
			continue
		}

		_, err := io.WriteString(args.output, t+"\n")
		if err != nil {
			return fmt.Errorf("write error: %w", err)
		}
	}

	return nil
}
