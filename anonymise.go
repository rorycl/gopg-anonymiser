package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// anonArgs is the Anonymise function signature
type anonArgs struct {
	dumpFilePath string    // a postgresql dump file via either os.Stdin or a file
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
	if err != nil {
		return fmt.Errorf("load filters error %w", err)
	}

	// refTables hold the processed reference table data
	refTables := RefTableRegister{}

	// scanDumpFile is a func that can be run in two modes: one for
	// reference mode, and another for standard, non-reference mode.
	// In reference mode two scans of the dumpfile are required: one for
	// collecting the reference tables in memory, then again to read the
	// tables again so that the references can be resolved.
	scanDumpFile := func(referenceMode bool, dumpFile io.Reader) error {

		// init a dump table and related filters
		dt := new(DumpTable)
		rdt := new(ReferenceDumpTable)

		var scanner *bufio.Scanner
		scanner = bufio.NewScanner(dumpFile)

		var lineNo = 0
		for scanner.Scan() {

			t := scanner.Text()

			if !dt.Inited() {

				// init dump table, which does not init if it returns a
				// sentinel error except for ErrIsRefDumpTable
				if referenceMode {
					rdt, err = NewReferenceDumpTable(t, tableFilters)
					dt = rdt.DumpTable
				}
				if !referenceMode {
					dt, err = NewDumpTable(t, referenceMode, tableFilters)
				}
				switch err {
				case ErrNoDumpTable:
				case ErrNotInterestingTable:
				case ErrIsNormalDumpTable: // normal in ref context
				case nil:
				default:
					return fmt.Errorf("Error parsing line %s : %w", t, err)
				}

				// re-initialise in-dump line numbers if the dumptable is
				// now initalised, add reference tables to the saving
				// map, and initialise any reference filters
				if dt.Inited() {

					// extract filters
					filters := tableFilters.getTableFilters(dt.TableName)
					if len(filters) == 0 {
						return fmt.Errorf("could not extract filters for table %s", dt.TableName)
					}

					lineNo = 0

					// load reference table into reference filter
					if !referenceMode {
						for _, f := range filters {
							f.setRefDumpTable(refTables)
						}
					}
				}

			} else {

				// the dump table is initialised; filter the lines unless
				// the end of table marker is found

				// however, in referenceMode, no output to args.output
				// is made, and lines are instead captured in
				// dt.orginalRows an dt.latestRows

				// count lines from 1
				lineNo++

				columns, ok := dt.LineSplitter(scanner.Text())
				if !ok {
					// register reference table in map
					if referenceMode {
						refTables[dt.TableName] = rdt
					}

					dt = new(DumpTable)
					if referenceMode {
						rdt = new(ReferenceDumpTable)
					}
					if args.changedOnly {
						continue
					}
					if !referenceMode {
						_, err := io.WriteString(args.output, t+"\n")
						if err != nil {
							return fmt.Errorf("write error: %w", err)
						}
					}
					continue
				}

				// make rows
				row := NewRow(dt, columns, lineNo)

				// filter rows first capturing the rows for reference
				// tables before and after filtering if applicable
				if referenceMode {
					copyCols := make([]string, len(row.Columns))
					copy(copyCols, row.Columns)
					origRow := NewRow(row.DumpTabler, copyCols, row.lineNo)
					rdt.originalRows = append(rdt.originalRows, origRow)
				}

				// extract filters

				filters := tableFilters.getTableFilters(dt.TableName)
				if len(filters) == 0 {
					return fmt.Errorf("could not extract filters for table %s", dt.TableName)
				}

				// process each row
				for _, f := range filters {
					row, err = f.Filter(row)
					if err != nil {
						return fmt.Errorf("filter error on table %s: %w", dt.TableName, err)
					}
				}

				if referenceMode {
					rdt.latestRows = append(rdt.latestRows, row)
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

			if !referenceMode {
				// output
				if args.changedOnly && !dt.Inited() {
					continue
				}
				_, err := io.WriteString(args.output, t+"\n")
				if err != nil {
					return fmt.Errorf("write error: %w", err)
				}
			}
		}
		return nil
	}

	fileOpener := func(path string) (io.Reader, error) {
		of, err := os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			return of, fmt.Errorf("could not open dumpfile at %s for reading: %w", path, err)
		}
		return of, err
	}

	// run reference table scan
	if len(tableFilters.refTableNames) > 0 {
		fo, err := fileOpener(args.dumpFilePath)
		if err != nil {
			return err
		}
		err = scanDumpFile(true, fo)
		if err != nil {
			return err
		}
	}

	// run standard scan
	fo, err := fileOpener(args.dumpFilePath)
	if err != nil {
		return err
	}
	err = scanDumpFile(false, fo)
	return err
}
