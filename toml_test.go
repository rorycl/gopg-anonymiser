package main

import (
	"testing"
)

func TestTomlSettings(t *testing.T) {

	lt, err := LoadToml("testdata/settings.toml")
	if err != nil {
		t.Errorf("Could not parse yaml %v", err)
	}

	// {Title:example settings file for gopg-anonymiser Tables:map[messages:{TableName:public.messages Filters:[{Column:all Filter:delete Source:}]} users:{TableName:public.users Filters:[{Column:name Filter:file_replace Source:/tmp/newnames.txt} {Column:password Filter:string_replace Source:this is a password}]}]} toml_test.go:17: {Title:example settings file for gopg-anonymiser Tables:map[messages:{TableName:public.messages Filters:[{Column:all Filter:delete Source:}]} users:{TableName:public.users Filters:[{Column:name Filter:file_replace Source:/tmp/newnames.txt} {Column:password Filter:string_replace Source:this is a password}]}]}

	if lt.Title != "example settings file for gopg-anonymiser" {
		t.Errorf("Title incorrect")
	}
	if len(lt.Tables["messages"].Filters) != 1 {
		t.Errorf("the messages table should have one filter")
	}
	if len(lt.Tables["users"].Filters) != 2 {
		t.Errorf("the users table should have two filters")
	}

	if lt.Tables["users"].Filters[1].Column != "password" {
		t.Errorf("the second users column should be 'password'")
	}
	if lt.Tables["users"].Filters[1].Filter != "string_replace" {
		t.Errorf("the second users filter should be string_replace")
	}
	if lt.Tables["users"].Filters[1].Source != "this is a password" {
		t.Errorf("the second users source should be 'this is a password'")
	}

	t.Logf("%+v\n", lt)
}
