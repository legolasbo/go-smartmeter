package smartmeter

import (
	"encoding/json"
	"fmt"
	"strings"
)

// All indicates that all datapoints should be retrieved. This is the default option.
const All DataRetrievalOption = 0

// Gas indicates that the gas usage should be retrieved.
const Gas DataRetrievalOption = 1

// Power indicates that the current power consumption should be retrieved.
const Power DataRetrievalOption = 2

// Totals indicates that the totals should be retrieved.
const Totals DataRetrievalOption = 4

// DataRetrievalOption is used to indicate which datapoints should be retrieved.
type DataRetrievalOption int

// NewDataRetrievalOption creates a new dataretrieval option from an integer.
func NewDataRetrievalOption(i int) DataRetrievalOption {
	if i >= int(Gas+Power+Totals) || i <= int(All) {
		return All
	}
	return DataRetrievalOption(i)
}

func ReadoutsToCSV(readouts []ReadoutData, retrieve DataRetrievalOption) []byte {
	header := getCSVHeader(retrieve)
	mapCallback := getReadoutToCSVFunction(retrieve)
	out := make([]string, len(readouts))
	for i, v := range readouts {
		out[i] = mapCallback(v)
	}

	return []byte(header + strings.Join(out, ""))
}

func getCSVHeader(retrieve DataRetrievalOption) string {
	header := "Timestamp"

	switch retrieve {
	case Gas:
		header += ",Gas received m3\n"
		break

	case Power:
		header += ",Power delivered kWh,Power received kWh\n"
		break

	case Totals:
		header += ",Total power delivered low tarif kWh,Total power delivered peak tarif kWh,Total power received low tarif kWh,Total power received peak tarif kWh\n"
		break

	case Gas + Power:
		header += ",Gas received m3,Power delivered kWh,Power received kWh\n"
		break

	case Gas + Totals:
		header += ",Gas received m3,Total power delivered low tarif kWh,Total power delivered peak tarif kWh,Total power received low tarif kWh,Total power received peak tarif kWh\n"
		break

	case Power + Totals:
		header += ",Power delivered kWh,Power received kWh,Total power delivered low tarif kWh,Total power delivered peak tarif kWh,Total power received low tarif kWh,Total power received peak tarif kWh\n"
		break

	default:
		header += ",Gas received m3,Power delivered kWh,Power received kWh,Total power delivered low tarif kWh,Total power delivered peak tarif kWh,Total power received low tarif kWh,Total power received peak tarif kWh\n"
	}
	return header
}

func getReadoutToCSVFunction(retrieve DataRetrievalOption) func(data ReadoutData) string {
	switch retrieve {
	case Gas:
		return readoutToCSVLineGas
	case Power:
		return readoutToCSVLinePower
	case Totals:
		return readoutToCSVLineTotals
	case Gas + Power:
		return readoutToCSVLineGasPower
	case Gas + Totals:
		return readoutToCSVLineGasTotals
	case Power + Totals:
		return readoutToCSVLinePowerTotals
	default:
		return readoutToCSVAll
	}
}

func readoutToCSVAll(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n", v.Timestamp, v.GasReceived, v.PowerDelivered, v.PowerReceived, v.TotalPowerDeliveredLowTarif, v.TotalPowerDeliveredPeakTarif, v.TotalPowerReceivedLowTarif, v.TotalPowerReceivedPeakTarif)
}

func readoutToCSVLinePowerTotals(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n", v.Timestamp, v.PowerDelivered, v.PowerReceived, v.TotalPowerDeliveredLowTarif, v.TotalPowerDeliveredPeakTarif, v.TotalPowerReceivedLowTarif, v.TotalPowerReceivedPeakTarif)
}

func readoutToCSVLineGasTotals(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f,%.3f,%.3f,%.3f,%.3f\n", v.Timestamp, v.GasReceived, v.TotalPowerDeliveredLowTarif, v.TotalPowerDeliveredPeakTarif, v.TotalPowerReceivedLowTarif, v.TotalPowerReceivedPeakTarif)
}

func readoutToCSVLineGasPower(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f,%.3f,%.3f\n", v.Timestamp, v.GasReceived, v.PowerDelivered, v.PowerReceived)
}

func readoutToCSVLineTotals(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f,%.3f,%.3f,%.3f\n", v.Timestamp, v.TotalPowerDeliveredLowTarif, v.TotalPowerDeliveredPeakTarif, v.TotalPowerReceivedLowTarif, v.TotalPowerReceivedPeakTarif)
}

func readoutToCSVLinePower(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f,%.3f\n", v.Timestamp, v.PowerDelivered, v.PowerReceived)
}

func readoutToCSVLineGas(v ReadoutData) string {
	return fmt.Sprintf("%s,%.3f\n", v.Timestamp, v.GasReceived)
}

// ReadoutsToJSON converts a slice of ReadoutData to json whilst taking the given DataRetrievalOption into account.
func ReadoutsToJSON(readouts []ReadoutData, retrieve DataRetrievalOption) []byte {
	output := make([]interface{}, len(readouts))

	for k, v := range readouts {
		o := make(map[string]interface{})
		o["Timestamp"] = v.Timestamp

		switch retrieve {
		case Gas:
			addGasToMap(o, v)
			break

		case Power:
			addPowerToMap(o, v)
			break

		case Totals:
			addTotalsToMap(o, v)
			break

		case Gas + Power:
			addGasToMap(o, v)
			addPowerToMap(o, v)
			break

		case Gas + Totals:
			addGasToMap(o, v)
			addTotalsToMap(o, v)
			break

		case Power + Totals:
			addPowerToMap(o, v)
			addTotalsToMap(o, v)
			break
		}

		if len(o) > 1 {
			output[k] = o
			continue
		}

		output[k] = v
	}

	j, _ := json.Marshal(output)
	return j
}

func addGasToMap(m map[string]interface{}, r ReadoutData) {
	m["GasReceived"] = r.GasReceived
}

func addPowerToMap(m map[string]interface{}, r ReadoutData) {
	m["PowerReceived"] = r.PowerReceived
	m["PowerDelivered"] = r.PowerDelivered
}

func addTotalsToMap(m map[string]interface{}, r ReadoutData) {
	m["TotalPowerDeliveredLowTarif"] = r.TotalPowerDeliveredLowTarif
	m["TotalPowerDeliveredPeakTarif"] = r.TotalPowerDeliveredPeakTarif
	m["TotalPowerReceivedLowTarif"] = r.TotalPowerReceivedLowTarif
	m["TotalPowerReceivedPeakTarif"] = r.TotalPowerReceivedPeakTarif
}
