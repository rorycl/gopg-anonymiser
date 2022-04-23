package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// anonArgs is the Anonymise function signature
type anonArgs struct {
	// a postgresql dump file via either os.Stdin or a file
	dumpFile io.Reader
	// a toml settings string
	settingsToml string
	// output to either os.Stdout or a file
	output io.Writer
	// only show changed tables inthe output
	changedOnly bool
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
