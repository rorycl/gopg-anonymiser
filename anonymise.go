package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

<<<<<<< HEAD
=======
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
				return rfs, fmt.Errorf("table %s: uuid filter error: %w", dt.TableName, err)
			}
			rfs = append(rfs, filter)

		case "string replace":
			if len(f.Columns) < 1 {
				return rfs, fmt.Errorf("table %s: string replace filter: must provide at lease one column", dt.TableName)
			}
			if len(f.Columns) != len(f.Replacements) {
				return rfs, fmt.Errorf("table %s: string replace filter: column length != replacement length", dt.TableName)
			}
			filter, err := NewReplaceFilter(
				f.Columns,
				f.Replacements,
				f.If,
				f.NotIf,
			)
			if err != nil {
				return rfs, fmt.Errorf("table %s: source error for string replace: %w", dt.TableName, err)
			}
			rfs = append(rfs, filter)

		case "file replace":
			if len(f.Columns) < 1 {
				return rfs, fmt.Errorf("table %s: file replace: must provide at lease one column", dt.TableName)
			}
			filer, err := os.Open(f.Source)
			if err != nil {
				return rfs, fmt.Errorf("table %s: file replace filter error: %w", dt.TableName, err)
			}
			filter, err := NewFileFilter(
				f.Columns,
				filer,
				f.If,
				f.NotIf,
			)
			if err != nil {
				return rfs, fmt.Errorf("table %s: source error for file error: %w", dt.TableName, err)
			}
			rfs = append(rfs, filter)

		default:
			return rfs, fmt.Errorf("table %s: filter type %s not known", dt.TableName, f.Filter)
		}
	}
	return rfs, nil
}

>>>>>>> main
// anonArgs is the Anonymise function signature
type anonArgs struct {
	dumpFile     io.Reader // a postgresql dump file via either os.Stdin or a file
	settingsToml string    // a toml settings string
	output       io.Writer // output to either os.Stdout or a file
	changedOnly  bool      // only show changed tables inthe output
}

// Anonymise anonymises a postgresql dump file
func Anonymise(args anonArgs) error {

	// load settings
	settings, err := LoadToml(args.settingsToml)
	if err != nil {
		return fmt.Errorf("settings load error %s", err)
	}

	// load filters
	tableFilters, err := loadFilters(settings)

	// init a dump table and related filters
	dt := new(DumpTable)

	scanner := bufio.NewScanner(args.dumpFile)

	var lineNo = 0
	for scanner.Scan() {

		t := scanner.Text()

		if !dt.Inited() {

			// try to init dt
			dt, err = NewDumpTable(t, tableFilters)
			switch err {
			case ErrNoDumpTable:
			case ErrNotInterestingTable:
			case nil:
			default:
				return fmt.Errorf("Error parsing line %s : %w", t, err)
			}

			// re-initialise in-dump line numbers if the dumptable is
			// now initalised
			if dt.Inited() {
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
			row := NewRow(dt, columns, lineNo)

			// filter rows
			for _, f := range dt.filters {
				row, err = f.Filter(row)
				if err != nil {
					return fmt.Errorf("filter error on table %s: %w", dt.TableName, err)
				}
			}
			// convert columns back to t unless the Row has been deleted
			if row.lineNo == 0 {
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
