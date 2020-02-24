package smartmeter

import "encoding/json"

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
