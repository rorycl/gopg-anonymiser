package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestFlagParsing(t *testing.T) {

	os.Args = []string{"prog", "-s", "settings.toml"}
	_, err := parseFlags()
	if err != nil {
		t.Errorf("failed flag parsing %s", err)
	}
}

func TestFlagParsingFail(t *testing.T) {

	os.Args = []string{"prog", "-s", "settings.toml", "/xyz/unlikely.name"}
	_, err := parseFlags()
	if err == nil {
		t.Errorf("flag parsing should have failed %s", err)
	}
	t.Log(err)
}

func TestFlagParsingOutputStdout(t *testing.T) {

	os.Args = []string{"prog", "-s", "settings.toml", "-o", "-"}
	_, err := parseFlags()
	if err != nil {
		t.Error("stdout output file should not fail")
	}
	t.Log(err)
}

func TestFlagParsingOutputFile(t *testing.T) {

	outFile, err := ioutil.TempFile("/tmp/", "out_")
	if err != nil {
		t.Error("could not make output tempfile")
	}
	defer os.Remove(outFile.Name())

	os.Args = []string{"prog", "-s", "settings.toml", "-o", outFile.Name()}
	_, err = parseFlags()
	if err != nil {
		t.Error("temporary file output should not fail")
	}
	t.Log(err)
}
