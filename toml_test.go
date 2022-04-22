package main

import (
	"os"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

var testSettings = "testdata/settings.toml"

func TestTomlSettings(t *testing.T) {

	filer, err := os.ReadFile(testSettings)
	if err != nil {
		t.Errorf("could not read test file: %s", err)
	}

	toml, err := LoadToml(string(filer))
	if err != nil {
		t.Errorf("Could not parse toml %v", err)
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

func TestTomlSettingsWithReferences(t *testing.T) {

	settings := `
[["example_schema.events"]]
filter = "delete"

[["public.users"]]
filter = "string replace"
columns = ["password"]
replacements = ["$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC"]
notif = {"lastname" = "langoustine"}

[["public.users"]]
filter = "file replace"
columns = ["firstname", "lastname"]
source = "testdata/newnames.txt"

[["public.users"]]
filter = "uuid"
columns = ["uuid"]

[["public.users"]]
filter = "file replace"
columns = ["notes"]
source = "testdata/newnotes.txt"
notif = {"notes" = '\N'}

[["public.needs_users"]]
filter = "reference replace"
columns = ["public.users.firstname", "public.users.lastname"]
# optargs = {"reference_relation" = {"public.users.id" = "lastname"}}
optargs = {"fklookup" = ["public.users.id", "lastname"]}
`
	toml, err := LoadToml(settings)
	if err != nil {
		t.Errorf("Could not parse toml %v", err)
	}
	needsUsers, ok := toml["public.needs_users"]
	if !ok {
		t.Errorf("public.needs_users could not be found")
	}
	if len(needsUsers) != 1 {
		t.Errorf("public.needs_users should have one filter")
	}
	filter := needsUsers[0]
	refRel, ok := filter.OptArgs["fklookup"]
	if !ok {
		t.Errorf("could not find optargs.fklookup")
	}
	if refRel[0] != "public.users.id" || refRel[1] != "lastname" {
		t.Errorf("refRel has incorrect values, got %v", refRel)
	}
}
