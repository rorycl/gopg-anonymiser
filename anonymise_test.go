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

	// COPY example_schema.events (id, flags, data) FROM stdin;
	// COPY public.fkexample (id, user_id, firstname_materialized) FROM stdin;
	// 1	1	zachary
	// 2	3	xavier
	// 3	5	vanessa
	// COPY public.users (id, firstname, lastname, password, uuid, notes) FROM stdin;
	// 1	zachary	zaiden	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	69000fae-ba42-413e-9346-b08a16be0858	\N
	// 2	yael	yaeger	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	a7157556-9e42-412b-9bdc-d12b753f0627	\N
	// 3	xavier	xander	$2a$06$.d8FVKIVagQaHU.6ouHGKegL85H8.cFIvXDNGC/wb8dXAWt3fmukq	df1085f3-2805-45a6-b50f-8ab94001870c	\N
	// 4	william	williamson	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	1dcd41f2-c5a6-47b0-9b76-273ef3b79c85	this is a second note\twith a tab
	// 5	vanessa	vaccarelli	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	855a7f35-b6aa-448f-b08d-7cb0679348df	this is the first note
	// 6	zachary	zaiden	$2a$06$.wHg4l7yz1ijSfMwa7fNruq3ASx1plpkC.XcI1wXdghCb4ZJQsrtC	89ba3d3b-687c-48c1-975b-a9ac5ef957b2	this is a second note\twith a tab

	// there should be 3 zacharies; one from public.fkexample, two from
	// public.users
	// vanessa should be on line 3 of public.fkexample

	var count int
	if count = strings.Count(buffer.String(), "zachary"); count != 3 {
		t.Errorf("count of zachary string not 3, got %d", count)
	}
	if count = strings.Count(buffer.String(), "3	5	vanessa"); count != 1 {
		t.Errorf("third row data for public.fkexample incorrect; row count %d", count)
	}
	if count = strings.Count(buffer.String(), "COPY "); count != 3 {
		t.Errorf("count of COPY lines should be 3, got %d", count)
	}
	if count = strings.Count(buffer.String(), "this is a second note"); count != 2 {
		t.Errorf("count of second note not 2, got %d", count)
	}

	t.Log(buffer.String())
}
