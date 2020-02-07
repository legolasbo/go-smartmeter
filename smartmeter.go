package main

import (
	"log"
	"github.com/tarm/serial"
)

func main() {
	c := &serial.Config{Name: "/dev/ttyUSB1", Baud: 115200}
	s, err := serial.OpenPort(c)
	if err != nil {
			log.Fatal(err)
	}
	
	buf := make([]byte, 128)
	n, err := s.Read(buf)
	if err != nil {
			log.Fatal(err)
	}
	log.Printf("%q", buf[:n])
}
