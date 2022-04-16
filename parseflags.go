package main

import (
	"fmt"
	"io"
	"os"

	flags "github.com/jessevdk/go-flags"
)

var usage = `: a simple postgresql dump file anonymiser.

Anonymise a postgresql dump file using a toml settings file setting out
the deletion, string_replace and file_replace filters to use.

gopg-anonymise -s <settings.toml> [-o output or stdout] [-t test]`

// Options set the programme flag options
type Options struct {
	Settings string `short:"s" long:"settings" required:"true" description:"settings toml file"`
	Output   string `short:"o" long:"output" description:"output file (otherwise stdout)"`
	Test     bool   `short:"t" long:"testmode" description:"show only changed lines for testing"`
	Args     struct {
		Input string `default:"" description:"input file or stdin"`
	} `positional-args:"yes"`
}

// parseFlags parses the command line options, taken out of main to
// allow testing
func parseFlags() (input io.Reader, settings string, output io.Writer, test bool, err error) {

	var options Options
	var parser = flags.NewParser(&options, flags.Default)
	parser.Usage = fmt.Sprintf(usage)

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	test = options.Test
	settings = options.Settings

	// open stdin or file for reading
	if options.Args.Input == "" {
		input = os.Stdin
	} else {
		filer, err := os.Open(options.Args.Input)
		if err != nil {
			return input, settings, output, test,
				fmt.Errorf("Could not open file %s for reading, %s", options.Args.Input, err)
		}
		input = filer
	}

	// open stdout or file for writing
	if options.Output == "" {
		output = os.Stdout
	} else {
		filer, err := os.Create(options.Output)
		if err != nil {
			return input, settings, output, test,
				fmt.Errorf("Could not create file %s, %s", options.Output, err)
		}
		output = filer
	}

	return input, settings, output, test, nil
}
