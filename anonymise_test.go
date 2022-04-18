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

	// the row with langoustine should not be changed to the new
	// password; note that langoustine is changed to
	contents := strings.SplitN(buffer.String(), "$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC", -1)
	if len(contents) != 6 {
		t.Errorf("split contents should provide 6 parts, returned %d", len(contents))
	}
	lines := strings.Split(buffer.String(), "\n")
	for i, l := range lines {
		if strings.Contains(l, "xander") {
			if !strings.Contains(l, "$2a$06$cj4Coa76ZPud2KiFW4wPDuTL98N8p4mFjJoV5mJ2Id9.2QiAcJ6bO") {
				t.Errorf("langoustine/xander password should not change on row %d", i)
			}
		}
	}
	count := strings.Count(buffer.String(), "zachary")
	if count != 2 {
		t.Errorf("count of zachary string not 2, got %d", count)
	}
	count = strings.Count(buffer.String(), "a third note")
	if count != 2 {
		t.Errorf("count of 'a third note' not 2, got %d", count)
	}

	t.Log(buffer.String())
}

func TestLoadFilters(t *testing.T) {

	settings := Settings{
		"a": []Filter{
			Filter{
				Filter:       "string replace",
				Columns:      []string{"a", "c"},
				Replacements: []string{"abc", "def"},
			},
		},
		"b": []Filter{
			Filter{
				Filter:  "uuid",
				Columns: []string{"b", "d"},
			},
		},
	}

	dt := DumpTable{TableName: "b", ColumnNames: []string{"a", "b", "c", "d"}}

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

func TestLoadFiltersFail(t *testing.T) {

	// all tests should fail
	tests := []struct {
		name    string
		setting Settings
	}{
		{
			name: "string replace should fail with no columns",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:       "string replace",
						Columns:      []string{},
						Replacements: []string{"abc", "def"},
					},
				},
			},
		},
		{
			name: "string replace should fail with col len != replacement len",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:       "string replace",
						Columns:      []string{"a", "c", "d"},
						Replacements: []string{"abc", "def"},
					},
				},
			},
		},
		{
			name: "uuid replace should fail with no columns",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:  "uuid",
						Columns: []string{},
					},
				},
			},
		},
		{
			name: "file replace should fail with no columns",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:  "file replace",
						Columns: []string{},
						Source:  "/dev/random",
					},
				},
			},
		},
		{
			name: "file replace should fail with no source",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:  "file replace",
						Columns: []string{"a", "c"},
						Source:  "",
					},
				},
			},
		},
	}

	dt := DumpTable{TableName: "b", ColumnNames: []string{"a", "b", "c", "d"}}

	for _, tc := range tests {
		_, err := loadFilters(tc.setting, &dt)
		if err == nil {
			t.Errorf("test %s failed: %s", tc.name, err)
		}
		t.Log(err)
	}
}
