package main

import (
	"log"
)

func main() {

	err := Anonymise("testdata/pg_dump.sql", "testdata/settings.toml")
	if err != nil {
		log.Fatal(err)
	}

}
