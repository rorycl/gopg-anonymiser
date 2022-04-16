package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestAnonymiseNoFail(t *testing.T) {

	dumpFile, err := ioutil.TempFile("/tmp/", "dump_")
	if err != nil {
		t.Error("could not make dump tempfile")
	}
	defer os.Remove(dumpFile.Name())

	settingsFile, err := ioutil.TempFile("/tmp/", "setting_")
	if err != nil {
		t.Error("could not make setting tempfile")
	}
	defer os.Remove(settingsFile.Name())

	args := anonArgs{
		dumpFile:     dumpFile,
		settingsFile: settingsFile.Name(),
		output:       os.Stdout,
		changedOnly:  false,
	}

	err = Anonymise(args)
	if err != nil {
		t.Error("empty files should not fail")
	}
}

func TestAnonymiseFail(t *testing.T) {

	dumpFile, err := ioutil.TempFile("/tmp/", "dump_")
	if err != nil {
		t.Error("could not make dump tempfile")
	}
	defer os.Remove(dumpFile.Name())

	settingsFile := "/tmp/ghi/jkl.toml"

	args := anonArgs{
		dumpFile:     dumpFile,
		settingsFile: settingsFile,
		output:       os.Stdout,
		changedOnly:  false,
	}

	err = Anonymise(args)
	if err == nil {
		t.Error("nonsense toml files should fail")
	}
	t.Logf("nonsense file error %s", err)
}

func TestAnonymiseOK(t *testing.T) {

	dumpFile := "testdata/pg_dump.sql"
	df, err := os.Open(dumpFile)
	if err != nil {
		t.Errorf("Could not open test dump file %s, %s", dumpFile, err)
	}

	settingsFile := "testdata/settings.toml"

	buffer := bytes.NewBuffer(nil)

	args := anonArgs{
		dumpFile:     df,
		settingsFile: settingsFile,
		output:       buffer,
		changedOnly:  true,
	}

	err = Anonymise(args)
	if err != nil {
		t.Errorf("Anonymise should not fail: %s", err)
	}

	contents := strings.SplitN(buffer.String(), "$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC", -1)
	if len(contents) != 7 {
		t.Errorf("split contents should provide 7 parts, returned %d", len(contents))
	}
	count := strings.Count(buffer.String(), "dilara")
	if count != 3 {
		t.Errorf("count of dilara string not 3, got %d", count)
	}
	count = strings.Count(buffer.String(), "a third note")
	if count != 2 {
		t.Errorf("count of 'a third note' not 2, got %d", count)
	}

	t.Log(buffer.String())
}

func TestLoadFilters(t *testing.T) {

	settings := Settings{
		Title: "test",
		Tables: map[string]SettingTable{
			"a": SettingTable{
				TableName: "tableA",
				Filters: []filters{
					filters{
						Column: "a",
						Filter: "string_replace",
						Source: "abc",
					},
				},
			},
			"b": SettingTable{
				TableName: "tableB",
				Filters: []filters{
					filters{
						Column: "b",
						Filter: "uuid_replace",
						Source: "",
					},
				},
			},
		},
	}

	dt := DumpTable{TableName: "tableB", ColumnNames: []string{"a", "b", "c"}}

	rowFilters, err := loadFilters(settings, &dt)
	if err != nil {
		t.Errorf("load filter error %s", err)
	}
	if len(rowFilters) != 1 {
		t.Errorf("length of rowfilters should be 1, is %d", len(rowFilters))
	}
	if rowFilters[0].FilterName() != "uuid replace" {
		t.Errorf("filter name not uuid replace, got %s", rowFilters[0].FilterName())
	}
	t.Logf("rowFilters: %T %+v\n", rowFilters, rowFilters)

}
