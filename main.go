package main

import (
	"fmt"
	"io"
	"os"

	flags "github.com/jessevdk/go-flags"
)

var usage = `:
a simple postgresql dump file anonymiser.

Anonymise a postgresql dump file using a toml settings file setting out
the deletion, string_replace and file_replace filters to use.

gopg-anonymise -s <settings.toml> [-o output or stdout] [-t test] [file or stdin]

`

// Options set the programme flag options
type Options struct {
	Settings string `short:"s" long:"settings" required:"true" description:"settings toml file"`
	Output   string `short:"o" long:"output" description:"output file (otherwise stdout)"`
	Test     bool   `short:"t" long:"testmode" description:"show only changed lines for testing"`
	Args     struct {
		Input string `default:"" description:"input file or stdin"`
	} `positional-args:"yes"`
}

func main() {

	var options Options
	var parser = flags.NewParser(&options, flags.Default)
	parser.Usage = fmt.Sprintf(usage)

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	var err error
	// open stdin or file for reading
	var input io.Reader
	if options.Args.Input == "" {
		input = os.Stdin
	} else {
		filer, err := os.Open(options.Args.Input)
		if err != nil {
			fmt.Printf("Could not open file %s for reading, %s", options.Args.Input, err)
			os.Exit(1)
		}
		defer filer.Close()
		input = filer
	}

	// open stdout or file for writing
	var output io.Writer
	if options.Output == "" {
		output = os.Stdout
	} else {
		filer, err := os.Create(options.Output)
		if err != nil {
			fmt.Printf("Could not create file %s, %s", options.Output, err)
			os.Exit(1)
		}
		defer filer.Close()
		output = filer
	}

	// run anonymiser
	err = Anonymise(input, options.Settings, output, options.Test)
	if err != nil {
		fmt.Printf("Anonymisation error: %s\n", err)
		os.Exit(1)
	}

}
