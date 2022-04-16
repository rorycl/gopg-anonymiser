package main

import (
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
