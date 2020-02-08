package main

import (
	"bufio"
	"fmt"
	"log"

	"github.com/tarm/serial"

	dsmr "github.com/legolasbo/go-dsmr"
)

func main() {
	rChan := make(chan string)
	tChan := make(chan string)
	go readLines(rChan)
	go collectTelegrams(rChan, tChan)
	parseTelegrams(tChan)
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

		telegram += line

		if line[0] == '!' {
			tChan <- telegram
		}
	}
}

func parseTelegrams(tChan chan string) {
	for t := range tChan {
		telegram, err := dsmr.ParseTelegram(t)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
			continue
		}

		printTelegram(telegram)
	}
}

func printTelegram(t dsmr.Telegram) {
	delivered, _ := t.ActualElectricityPowerDelivered()
	fmt.Println("Actual electricity delivered", delivered, "kw")
	received, _ := t.ActualElectricityPowerReceived()
	fmt.Println("Actual electricity received", received, "kw")
	gas1, _ := t.MeterReadingGasDeliveredToClient(1)
	fmt.Println("Gas delivered on channel 1", gas1, "m3")
	gas2, _ := t.MeterReadingGasDeliveredToClient(2)
	fmt.Println("Gas delivered on channel 2", gas2, "m3")
}
