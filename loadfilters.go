package main

import (
	"errors"
	"fmt"
	"os"
)

// tableFilters represent a map of fully qualified table names (in
// schema.table format) to a list of filters represented by the common
// interface used by filters, together with a slice of reference table
// names, those tables referred to by other tables by reference filters
type tableFilters struct {
	refTableNames []string
	tableFilters  map[string][]RowFilterer
}

// loadFilters loads a set of filters from a settings file and returns a
// tableFilters struct
func loadFilters(settings Settings) (tableFilters, error) {

	tf := tableFilters{
		refTableNames: []string{},
		tableFilters:  map[string][]RowFilterer{},
	}

	// retrieve filters for each table from settings
	for tableName, filters := range settings {

		rfs := []RowFilterer{}

		if len(filters) == 0 {
			return tf, fmt.Errorf("table '%s' could not be found in settings", tableName)
		}

		// load filters
		for _, f := range filters {

			switch f.Filter {
			case "delete":
				filter, _ := NewDeleteFilter()
				rfs = append(rfs, filter)

			case "uuid":
				filter, err := NewUUIDFilter(f.Columns, f.If, f.NotIf)
				if err != nil {
					return tf, fmt.Errorf("uuid filter error: %w", err)
				}
				rfs = append(rfs, filter)

			case "string replace":
				if len(f.Columns) < 1 {
					return tf, errors.New("string replace filter: must provide at lease one column")
				}
				if len(f.Columns) != len(f.Replacements) {
					return tf, errors.New("string replace filter: column length != replacement length")
				}
				filter, err := NewReplaceFilter(
					f.Columns,
					f.Replacements,
					f.If,
					f.NotIf,
				)
				if err != nil {
					return tf, fmt.Errorf("source error for string replace: %w", err)
				}
				rfs = append(rfs, filter)

			case "file replace":
				if len(f.Columns) < 1 {
					return tf, errors.New("file replace: must provide at lease one column")
				}
				filer, err := os.Open(f.Source)
				if err != nil {
					return tf, fmt.Errorf("file replace filter error: %w", err)
				}
				filter, err := NewFileFilter(
					f.Columns,
					filer,
					f.If,
					f.NotIf,
				)
				if err != nil {
					return tf, fmt.Errorf("source error for file error: %w", err)
				}
				rfs = append(rfs, filter)

			case "reference replace":

				fk, ok := f.OptArgs["fklookup"]
				if !ok {
					fmt.Errorf("no optargs.fklookup provided for %s", tableName)
				}
				fkKeyCol := fk[0]
				fkValueCol := fk[1]

				filter, err := NewReferenceFilter(
					f.Columns,
					f.Replacements,
					f.If,
					f.NotIf,
					fkKeyCol,
					fkValueCol,
				)
				if err != nil {
					return tf, fmt.Errorf("creation error for reference replace: %w", err)
				}
				rfs = append(rfs, filter)

			default:
				return tf, fmt.Errorf("filter type %s not known", f.Filter)
			}
		}
		// assign filters for this table to the tableFilters map entry
		tf.tableFilters[tableName] = rfs
	}

	// check the filters
	if err := tf.check(); err != nil {
		return tf, err
	}

	return tf, nil
}

// check if the filters for each table are ok as a group, and calculate
// the
func (t *tableFilters) check() error {

	if len(t.tableFilters) == 0 {
		return errors.New("tableFilters have no entries")
	}

	// a map of reference tables and source tables
	var refTables = make(map[string]int)
	var sourceTables = make(map[string]int)

	for table, filters := range t.tableFilters {
		l := len(filters)
		for _, f := range filters {

			switch f.FilterName() {
			case "delete":
				if l != 1 {
					return fmt.Errorf("delete filter used with another filter for %s", table)
				}

			case "reference replace":
				rf, ok := f.(ReferenceFilter)
				if !ok {
					fmt.Errorf("could not extract reference filter for %s", table)
				}
				refTables[rf.fkTableName]++
				sourceTables[table]++
			}
		}
	}

	// ensure there are no circular references, and assign reference
	// table entries to the t.refTableNames entry
	for r := range refTables {
		t.refTableNames = append(t.refTableNames, r)
		for s := range sourceTables {
			if s == r {
				return fmt.Errorf("table %s used for both source and reference table", s)
			}
		}
	}

	return nil
}
