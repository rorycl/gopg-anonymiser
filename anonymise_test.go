package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestAnonymiseEmptyFail(t *testing.T) {

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

	tomlString, err := io.ReadAll(settingsFile)
	if err != nil {
		t.Error("could not read temp file")
	}

	args := anonArgs{
		dumpFilePath: dumpFile.Name(),
		settingsToml: string(tomlString),
		output:       os.Stdout,
		changedOnly:  false,
	}

	err = Anonymise(args)
	if err == nil {
		t.Error("empty files should fail")
	}
	t.Log(err)
}

func TestAnonymiseFail(t *testing.T) {

	dumpFile, err := ioutil.TempFile("/tmp/", "dump_")
	if err != nil {
		t.Error("could not make dump tempfile")
	}
	defer os.Remove(dumpFile.Name())

	settingsToml := "xxx yyyy zzz"

	args := anonArgs{
		dumpFilePath: dumpFile.Name(),
		settingsToml: settingsToml,
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

	tomlString, err := os.ReadFile("testdata/settings.toml")
	if err != nil {
		t.Errorf("could not read settings file: %w", err)
	}

	buffer := bytes.NewBuffer(nil)

	args := anonArgs{
		dumpFilePath: dumpFile,
		settingsToml: string(tomlString),
		output:       buffer,
		changedOnly:  true,
	}

	err = Anonymise(args)
	if err != nil {
		t.Errorf("Anonymise should not fail: %s", err)
	}

	// the row with langoustine should not be changed to the new
	// password; note that langoustine is changed to
	pw := "$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC"
	contents := strings.SplitN(buffer.String(), pw, -1)
	if len(contents) != 6 {
		t.Errorf("split contents should provide 6 parts, returned %d", len(contents))
	}
	lines := strings.Split(buffer.String(), "\n")
	for i, l := range lines {
		if strings.Contains(l, "xander") {
			if strings.Contains(l, pw) {
				t.Errorf("langoustine/xander password should not change on row %d", i)
			}
		}
	}
	count := strings.Count(buffer.String(), "zachary")
	if count != 2 {
		t.Errorf("count of zachary string not 2, got %d", count)
	}
	count = strings.Count(buffer.String(), "this is a second note")
	if count != 2 {
		t.Errorf("count of second note not 2, got %d", count)
	}

	t.Log(buffer.String())
}
