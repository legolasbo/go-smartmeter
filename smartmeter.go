package main

import (
	"bufio"
	"fmt"
	"log"

	"github.com/tarm/serial"
)

func main() {
	c := &serial.Config{Name: "/dev/ttyUSB1", Baud: 115200}
	s, err := serial.OpenPort(c)
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for (i < 10) {
		reader := bufio.NewReader(s)
		reply, err := reader.ReadBytes('/')
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("READ!")
		fmt.Println(reply)

		i++
	}
}
