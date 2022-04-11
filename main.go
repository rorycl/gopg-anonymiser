package main

import (
	"log"
	"os"
)

func main() {

	output := os.Stdout
	err := Anonymise("testdata/pg_dump.sql", "testdata/settings.toml", output, false)
	if err != nil {
		log.Fatal(err)
	}

}
