package smartmeter

import (
	"bufio"
	"log"

	"github.com/tarm/serial"

	dsmr "github.com/legolasbo/go-dsmr"
)

// AllowSerialPortFailure adds the option to continue if the serial connection fails.
var AllowSerialPortFailure = false

// ReadTelegrams reads telegrams from the given serial port into the given readout channel.
func ReadTelegrams(serialPort string, rChan chan Readout) {
	lineChan := make(chan string)
	rawTelegramChan := make(chan string)
	go readLines(serialPort, lineChan)
	go collectTelegrams(lineChan, rawTelegramChan)
	parseTelegrams(rawTelegramChan, rChan)
}

func readLines(serialPort string, rChan chan string) {
	c := &serial.Config{
		Name: serialPort,
		Baud: 115200,
	}

	s, err := serial.OpenPort(c)
	if err != nil {
		if AllowSerialPortFailure {
			return;
		}
		log.Fatal(err)
	}

	reader := bufio.NewReader(s)
	for {
		reply, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err)
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

		// We usually start halfway trough a telegram.
		// Which means that the first telegram would be corrupt.
		// We therefore ignore everything until the first telegram start.
		if !foundStart {
			continue
		}

		telegram += line

		// The last line of a telegram starts with an exclamation mark.
		if line[0] == '!' {
			tChan <- telegram
		}
	}
}

func parseTelegrams(rawTelegramChan chan string, rChan chan Readout) {
	for t := range rawTelegramChan {
		telegram, err := dsmr.ParseTelegram(t)
		if err != nil {
			log.Println(err)
			continue
		}

		rChan <- Readout{telegram}
	}
}
