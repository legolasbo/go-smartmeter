package smartmeter

import (
	"strconv"
	"time"

	"github.com/legolasbo/go-dsmr"
)

// Readout contains relevant information from a dsmr telegram.
type Readout struct {
	telegram dsmr.Telegram
}

// Timestamp returns the timestamp.
func (r *Readout) Timestamp() time.Time {
	return r.telegram.DateTime
}

// PowerDelivered returns the kilowatts delivered to the grid in 1 watt resolution.
func (r *Readout) PowerDelivered() float64 {
	raw, ok := r.telegram.ActualElectricityPowerDelivered()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// PowerReceived returns the kilowatts recieved from the grid in 1 watt resolution.
func (r *Readout) PowerReceived() float64 {
	raw, ok := r.telegram.ActualElectricityPowerReceived()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// GasReceived returns the m3 of gas received from the mains in 1mm3 resolution.
func (r *Readout) GasReceived(channel int) float64 {
	raw, ok := r.telegram.MeterReadingGasDeliveredToClient(channel)
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}
