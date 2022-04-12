package main

import (
	"fmt"
	"io"
	"os"

	flags "github.com/jessevdk/go-flags"
)

var usage = `

anonymise a postgresql dump file using a toml settings file setting out
the deletion, string_replace and file_replace filters to use.

`

// Options set the programme flag options
type Options struct {
	Settings string `short:"s" long:"settings" required:"true" description:"settings toml file"`
	Input    string `short:"i" long:"input" required:"true" description:"input file or - for stdin"`
	Test     bool   `short:"t" long:"testmode" description:"show only changed lines for testing"`
	Args     struct {
		Output string `description:"output file or - for stdout"`
	} `positional-args:"yes" required:"yes"`
}

func main() {

	var options Options
	var parser = flags.NewParser(&options, flags.Default)
	parser.Usage = fmt.Sprintf(usage)

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	// open stdout or file for writing
	var output io.Writer
	var err error
	if options.Args.Output == "-" {
		output = os.Stdout
	} else {
		filer, err := os.Create(options.Args.Output)
		if err != nil {
			fmt.Printf("Could not create file %s, %s", output, err)
			os.Exit(1)
		}
		defer filer.Close()
		output = filer
	}

	// run anonymiser
	err = Anonymise(options.Input, options.Settings, output, options.Test)
	if err != nil {
		fmt.Printf("Anonymisation error: %w\n", err)
	}

}
