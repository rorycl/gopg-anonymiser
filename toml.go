package main

import (
	"github.com/BurntSushi/toml"
)

// TableSettings describes types of tables described in the settings
// toml file
type TableSettings struct {
	Title  string
	Tables map[string]table
}

// Table structure
type table struct {
	TableName string
	Filters   []filters
}

// Filters structure
type filters struct {
	Column string
	Filter string
	Source string
}

// LoadToml loads a toml file and returns a Tables structure
func LoadToml(file string) (TableSettings, error) {

	var tables TableSettings
	_, err := toml.DecodeFile(file, &tables)
	if err != nil {
		return tables, err
	}
	return tables, nil
}
