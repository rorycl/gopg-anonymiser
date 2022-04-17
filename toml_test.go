package main

import (
	"strings"
	"testing"
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

	/*
		test the second filter
		[["public.users"]]
		filter = "string replace"
		columns = ["password"]
		# give all users the same password
		replacements = ["$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC"]
	*/

	if toml["public.users"][1].Columns[0] != "password" {
		t.Errorf("the second users target column should be 'password'")
	}
	if toml["public.users"][1].Filter != "string replace" {
		t.Errorf("the second users filter should be string_replace")
	}
	if !strings.Contains(toml["public.users"][1].Replacements[0], "$2a$06$.wHg4l7") {
		t.Errorf("the second users source should start '$2a$06$.wHg4l7'")
	}
	t.Logf("%+v\n", toml)
}
