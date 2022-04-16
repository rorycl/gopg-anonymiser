package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// loadFilters loads a set of table transformation filters for a dump
// table from settings
func loadFilters(settings Settings, dt *DumpTable) ([]RowFilterer, error) {

	rfs := []RowFilterer{}
	tableName := dt.TableName

	// retrieve settings for this dump table
	var settingTable SettingTable
	for _, t := range settings.Tables {
		if t.TableName == tableName {
			settingTable = t
			break
		}
	}
	if settingTable.TableName == "" {
		return rfs, fmt.Errorf("table '%s' could not be found in settings", tableName)
	}

	// load filters
	for _, f := range settingTable.Filters {

		switch f.Filter {
		case "delete":
			filter, _ := NewRowDeleteFilter()
			rfs = append(rfs, filter)

		case "string_replace":
			filter, err := NewRowStringReplaceFilter(f.Column, f.Source)
			if err != nil {
				return rfs, fmt.Errorf("load error for string_replace : %w", err)
			}
			rfs = append(rfs, filter)

		case "file_replace":
			filer, err := os.Open(f.Source)
			if err != nil {
				return rfs, fmt.Errorf("source error for file_replace : %w", err)
			}
			filter, err := NewRowFileReplaceFilter(f.Column, filer)
			if err != nil {
				return rfs, fmt.Errorf("load error for file_replace : %w", err)
			}
			rfs = append(rfs, filter)

		case "uuid_replace":
			filter, err := NewRowFilterUUIDFilter(f.Column)
			if err != nil {
				return rfs, fmt.Errorf("source error for uuid_replace : %w", err)
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
	dumpFile     io.Reader
	settingsFile string
	output       io.Writer
	changedOnly  bool
}

// Anonymise anonymises a postgresql dump file
func Anonymise(args anonArgs) error {

	// load settings
	settings, err := LoadToml(args.settingsFile)
	if err != nil {
		return fmt.Errorf("settings file load error %s", err)
	}

	interestingTables := []string{}
	for _, t := range settings.Tables {
		interestingTables = append(interestingTables, t.TableName)
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
					return fmt.Errorf("filter error %w", err)
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
