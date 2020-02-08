package main

import (
	"bufio"
	"fmt"
	"log"

	"github.com/tarm/serial"
)

func main() {
		rChan := make(chan string)
	tChan := make(chan string)
	go readLines(rChan)
	go collectTelegrams(rChan, tChan)
	go parseTelegrams(tChan)
}

func readLines(rChan chan string) {
	c := &serial.Config{Name: "/dev/ttyUSB0", Baud: 115200}
	s, err := serial.OpenPort(c)
	if err != nil {
		log.Fatal(err)
	}
	
	reader := bufio.NewReader(s)
	for {
		reply, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
			continue
		}
	
		rChan <- reply
	}
}

func collectTelegrams(rChan chan string, tChan chan string) {
	telegram := ""
	foundStart := false

	for line := range rChan {
		if line[0] == '/' {
			foundStart = true
			telegram = ""
		}

		if !foundStart {
			continue
		}

		telegram += line;

		if line[0] == '!' {
			tChan <- telegram
		}
	}
}

func parseTelegrams(tChan chan string) {
	for t := range tChan {
		fmt.Println(t)
	}
}
