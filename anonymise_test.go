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

	err = Anonymise(dumpFile.Name(), settingsFile.Name(), os.Stdout, false)
	if err != nil {
		t.Error("empty files should not fail")
	}
}

func TestAnonymiseFail(t *testing.T) {

	dumpFile := "/tmp/abc/def.sql"
	settingsFile := "/tmp/ghi/jkl.toml"

	err := Anonymise(dumpFile, settingsFile, os.Stdout, false)
	if err == nil {
		t.Error("nonsense settings and toml files should fail")
	}
	t.Errorf("nonsense file error %s", err)
}

func TestAnonymiseOK(t *testing.T) {

	dumpFile := "testdata/pg_dump.sql"
	settingsFile := "testdata/settings.toml"

	buffer := bytes.NewBuffer(nil)

	err := Anonymise(dumpFile, settingsFile, buffer, true)
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
