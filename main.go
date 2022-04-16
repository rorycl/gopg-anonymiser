package main

import (
	"fmt"
	"os"
)

func main() {

	// parse flags
	dumpFile, settings, output, changedOnly, err := parseFlags()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// run anonymiser
	err = Anonymise(dumpFile, settings, output, changedOnly)
	if err != nil {
		fmt.Printf("Anonymisation error: %s\n", err)
		os.Exit(1)
	}
}
