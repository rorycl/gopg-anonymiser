package main

import (
	"github.com/BurntSushi/toml"
)

// Settings describes types of tables described in the settings
// toml file
type Settings struct {
	Title  string
	Tables map[string]SettingTable
}

// SettingTable sets out the filters for a Table
type SettingTable struct {
	TableName string
	Filters   []filters
}

// Filters structure
type filters struct {
	Filter       string
	Columns      []string
	Replacements []string
	Source       string
}

// LoadToml loads a toml file and returns a Settings structure
func LoadToml(file string) (Settings, error) {

	var tables Settings
	_, err := toml.DecodeFile(file, &tables)
	if err != nil {
		return tables, err
	}
	return tables, nil
}
