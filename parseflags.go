package main

import (
	"fmt"
	"os"

	flags "github.com/jessevdk/go-flags"
)

var usage = `: a simple postgresql dump file anonymiser.

Anonymise a postgresql dump file using a toml settings file setting out
the deletion, or columnar uuid, string or file filters to use.

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
func parseFlags() (args anonArgs, err error) {

	var options Options
	var parser = flags.NewParser(&options, flags.Default)
	parser.Usage = fmt.Sprintf(usage)

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	args.changedOnly = options.Test

	// set dumpfile
	if options.Args.Input == "" {
		args.dumpFilePath = "-" // set os.Stdout
	} else {
		args.dumpFilePath = options.Args.Input
	}

	// read settings file
	settings, err := os.ReadFile(options.Settings)
	if err != nil {
		return args, fmt.Errorf("Could not read settings file: %s", err)
	}
	args.settingsToml = string(settings)

	// open stdin or file for reading
	if options.Args.Input == "" {
		args.dumpFilePath = "-"
	}

	// open stdout or file for writing
	if options.Output == "" || options.Output == "-" {
		args.output = os.Stdout
	} else {
		filer, err := os.Create(options.Output)
		if err != nil {
			return args,
				fmt.Errorf("Could not create file %s, %s", options.Output, err)
		}
		args.output = filer
	}

	return args, nil
}
