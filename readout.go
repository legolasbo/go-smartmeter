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
	raw, ok := r.telegram.ActualElectricityPowerReceived()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// PowerReceived returns the kilowatts recieved from the grid in 1 watt resolution.
func (r *Readout) PowerReceived() float64 {
	raw, ok := r.telegram.ActualElectricityPowerDelivered()
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

// TotalPowerReceivedLowTarif returns the total power received in the peak tarif in kWh with 1 Wh resolution.
func (r *Readout) TotalPowerReceivedLowTarif() float64 {
	raw, ok := r.telegram.MeterReadingElectricityDeliveredToClientTariff1()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// TotalPowerReceivedPeakTarif returns the total power received in the peak tarif in kWh with 1 Wh resolution.
func (r *Readout) TotalPowerReceivedPeakTarif() float64 {
	raw, ok := r.telegram.MeterReadingElectricityDeliveredToClientTariff2()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// TotalPowerDeliveredLowTarif returns the total power received in the peak tarif in kWh with 1 Wh resolution.
func (r *Readout) TotalPowerDeliveredLowTarif() float64 {
	raw, ok := r.telegram.MeterReadingElectricityDeliveredByClientTariff1()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// TotalPowerDeliveredPeakTarif returns the total power received in the peak tarif in kWh with 1 Wh resolution.
func (r *Readout) TotalPowerDeliveredPeakTarif() float64 {
	raw, ok := r.telegram.MeterReadingElectricityDeliveredByClientTariff2()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(raw, 64)
	return f
}

// CurrentTarif returns the current tarif.
func (r *Readout) CurrentTarif() int64 {
	raw, ok := r.telegram.TariffIndicatorElectricity()
	if !ok {
		return 0
	}
	f, _ := strconv.ParseInt(raw, 10, 64)
	return f
}
