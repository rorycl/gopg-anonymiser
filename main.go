package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {

	filer, err := os.Open("/tmp/o")
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(filer)
	scanner.Split(bufio.ScanLines)

	p := false
	q := false
	var qHeader string
	var columns []string

	for scanner.Scan() {

		t := scanner.Text()

		// messages.messages
		if strings.Contains(t, "COPY messages.messages (") {
			// p = true
			p = false
			continue
		}
		if p && t == `\.` {
			p = false
			continue
		}
		if p == true {
			fmt.Println(t)
		}

		// rotadata.users
		identifier := `COPY rotadata.users (`
		if strings.Contains(t, identifier) {
			q = true
			qHeader = strings.ReplaceAll(t, identifier, "")
			qHeader = strings.ReplaceAll(qHeader, ") FROM stdin;", "")
			columns = strings.Split(qHeader, ", ")
			continue
		}
		if q && t == `\.` {
			q = false
			continue
		}
		if q == true {
			users := strings.Split(t, "\t")
			if len(users) > 6 {
				fmt.Printf("%-10s %s\n", columns[5], users[5])
			}
		}
	}

}
