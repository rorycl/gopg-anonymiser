package main

import (
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestTomlSettings(t *testing.T) {

	toml, err := LoadToml("testdata/settings.toml")
	if err != nil {
		t.Errorf("Could not parse yaml %v", err)
	}

	if len(toml["example_schema.events"]) != 1 {
		t.Errorf(
			"the events table should have one filter, got %d",
			len(toml["example_schema.events"]),
		)
	}
	if len(toml["public.users"]) != 4 {
		t.Errorf(
			"the users table should have four filters, got %d",
			len(toml["public.users"]),
		)
	}

	filterOne := toml["public.users"][0]
	if filterOne.Columns[0] != "password" {
		t.Errorf("the first users target column should be 'password'")
	}
	if filterOne.Filter != "string replace" {
		t.Errorf("the first users filter should be string_replace")
	}
	if !strings.Contains(filterOne.Replacements[0], "$2a$06$.wHg4l7") {
		t.Errorf("the first users filter source should start '$2a$06$.wHg4l7'")
	}
	if len(filterOne.If) != 0 {
		t.Error("the length of the first users if map should be 0")
	}
	if len(filterOne.NotIf) != 1 {
		t.Error("the length of the first users not if map should be 1")
	}
	if l, ok := filterOne.NotIf["lastname"]; !ok || l != "langoustine" {
		t.Error("first user filter notif does not have correct params")
	}
	if !strings.Contains(filterOne.Replacements[0], "$2a$06$.wHg4l7") {
		t.Errorf("the first users source should start '$2a$06$.wHg4l7'")
	}
	t.Logf("%+v\n", toml)
}

func TestTomlNULL(t *testing.T) {

	var filter Filter

	inlineToml := ` 
filter = "uuid"
columns = ["uuid"]
if = {"notes" = '\N'}
notif = {"notes" = "\\N"}`

	_, err := toml.Decode(inlineToml, &filter)
	if err != nil {
		t.Errorf("could not decode toml filter: %w", err)
	}
	if notes, ok := filter.If["notes"]; !ok || notes != `\N` {
		t.Errorf("null decoding if map failed: ok %t notes %s", ok, notes)
	}
	if notes, ok := filter.NotIf["notes"]; !ok || notes != `\N` {
		t.Errorf("null decoding notif map failed ok %t notes %s", ok, notes)
	}
}
