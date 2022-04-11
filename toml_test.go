package main

import (
	"strings"
	"testing"
)

func TestTomlSettings(t *testing.T) {

	lt, err := LoadToml("testdata/settings.toml")
	if err != nil {
		t.Errorf("Could not parse yaml %v", err)
	}

	// {Title:example settings file for gopg-anonymiser Tables:map[messages:{TableName:public.messages Filters:[{Column:all Filter:delete Source:}]} users:{TableName:public.users Filters:[{Column:name Filter:file_replace Source:/tmp/newnames.txt} {Column:password Filter:string_replace Source:this is a password}]}]} toml_test.go:17: {Title:example settings file for gopg-anonymiser Tables:map[messages:{TableName:public.messages Filters:[{Column:all Filter:delete Source:}]} users:{TableName:public.users Filters:[{Column:name Filter:file_replace Source:/tmp/newnames.txt} {Column:password Filter:string_replace Source:this is a password}]}]}

	if lt.Title != "test settings file for gopg-anonymiser" {
		t.Errorf("Title incorrect")
	}
	if len(lt.Tables["events"].Filters) != 1 {
		t.Errorf("the events table should have one filter")
	}
	if len(lt.Tables["users"].Filters) != 3 {
		t.Errorf("the users table should have three filters")
	}
	if lt.Tables["users"].Filters[1].Column != "password" {
		t.Errorf("the second users target column should be 'password'")
	}
	if lt.Tables["users"].Filters[1].Filter != "string_replace" {
		t.Errorf("the second users filter should be string_replace")
	}
	if !strings.Contains(lt.Tables["users"].Filters[1].Source, "$2a$06$.wHg4l7") {
		t.Errorf("the second users source should start '$2a$06$.wHg4l7'")
	}

	t.Logf("%+v\n", lt)
}
