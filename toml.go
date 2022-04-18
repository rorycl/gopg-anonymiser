package main

import (
	"github.com/BurntSushi/toml"
)

// Settings describes the settings of the project as a map with
// postgresql table names as keys with values as an array of filters
type Settings map[string][]Filter

// Filter structure
type Filter struct {
	Filter       string
	Columns      []string
	Replacements []string
	Source       string
	// conditionals
	If    map[string]string
	NotIf map[string]string
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
