package main

import (
	"errors"
	"fmt"
	"os"
)

// loadFilters loads a set of filters from a settings file and returns a
// map of table names to slices of RowFilterer, the common interface
// used by filters
func loadFilters(settings Settings) (map[string][]RowFilterer, error) {

	tableFilters := map[string][]RowFilterer{}

	// retrieve filters for each table from settings
	for tableName, filters := range settings {

		rfs := []RowFilterer{}

		if len(filters) == 0 {
			return tableFilters, fmt.Errorf("table '%s' could not be found in settings", tableName)
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
					return tableFilters, fmt.Errorf("uuid filter error: %w", err)
				}
				rfs = append(rfs, filter)

			case "string replace":
				if len(f.Columns) < 1 {
					return tableFilters, errors.New("string replace filter: must provide at lease one column")
				}
				if len(f.Columns) != len(f.Replacements) {
					return tableFilters, errors.New("string replace filter: column length != replacement length")
				}
				filter, err := NewReplaceFilter(
					f.Columns,
					f.Replacements,
					f.If,
					f.NotIf,
				)
				if err != nil {
					return tableFilters, fmt.Errorf("source error for string replace: %w", err)
				}
				rfs = append(rfs, filter)

			case "file replace":
				if len(f.Columns) < 1 {
					return tableFilters, errors.New("file replace: must provide at lease one column")
				}
				filer, err := os.Open(f.Source)
				if err != nil {
					return tableFilters, fmt.Errorf("file replace filter error: %w", err)
				}
				filter, err := NewFileFilter(
					f.Columns,
					filer,
					f.If,
					f.NotIf,
				)
				if err != nil {
					return tableFilters, fmt.Errorf("source error for file error: %w", err)
				}
				rfs = append(rfs, filter)

			case "reference replace":
				// todo
				rfs = append(rfs, mockFilter{})

			default:
				return tableFilters, fmt.Errorf("filter type %s not known", f.Filter)
			}
		}
		// assign filters for this table to the tableFilters map entry
		tableFilters[tableName] = rfs
	}
	return tableFilters, nil
}
