package smartmeter

import (
	"database/sql"
	"log"
	"math"
	"time"
)

// Storage provides an abstraction for the storage backend.
type Storage interface {
	// Insert inserts a readout into the storage backend.
	Insert(readout Readout)
	// GetRange retrieves a set of readouts within the given range.
	GetRange(start time.Time, end time.Time) ([]ReadoutData, error)
	// GetAveragedRange retrieves a set of readouts within the given range and averages them over a given interval.
	GetAveragedRange(start time.Time, end time.Time, interval time.Duration) ([]ReadoutData, error)
}

// SQL provides an SQL implementation of the storage backend.
type SQL struct {
	Storage
	initialized     bool
	Database        string
	db              *sql.DB
	insertStatement *sql.Stmt
}

func (s *SQL) initialize() {
	conn, err := sql.Open("mysql", s.Database)
	panicOnError(err)
	s.db = conn
	s.prepareTables()
	s.initializeInsertStatement()
	s.initialized = true

	go s.keepAlive()
}

func (s *SQL) prepareTables() {
	tables := []string{"readouts"}

	for _, table := range tables {
		if !s.tableExists(table) {
			s.createTable(table)
		}
	}
}

func (s *SQL) tableExists(tableName string) bool {
	rows, err := s.db.Query("SHOW TABLES")
	panicOnError(err)

	for rows.Next() {
		var row string
		rows.Scan(&row)
		if row == tableName {
			return true
		}
	}
	return false
}

func (s *SQL) createTable(tableName string) {
	var query string

	switch tableName {
	case "readouts":
		query = `CREATE TABLE readouts (
			id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
			timestamp DATETIME,
			date DATE,
			time TIME,
			tarif int,
			power_received FLOAT,
			power_deliverd FLOAT,
			gas_received FLOAT,
			total_power_received_low FLOAT,
			total_power_received_peak FLOAT,
			total_power_delivered_low FLOAT,
			total_power_delivered_peak FLOAT
			)`
	default:
		panic("Unknown table: " + tableName)
	}

	_, err := s.db.Exec(query)
	panicOnError(err)
}

func (s *SQL) initializeInsertStatement() {
	stmt, err := s.db.Prepare(`INSERT readouts SET 
			timestamp=?,
			date=?,
			time=?,
			tarif=?,
			power_received=?,
			power_deliverd=?,
			gas_received=?,
			total_power_received_low=?,
			total_power_received_peak=?,
			total_power_delivered_low=?,
			total_power_delivered_peak=?
	`)
	panicOnError(err)
	s.insertStatement = stmt
}

func (s *SQL) keepAlive() {
	ticks := time.NewTicker(time.Second * 30)
	for {
		s.db.Ping()
		<-ticks.C
	}
}

func (s *SQL) ensureInitialized() {
	if !s.initialized {
		s.initialize()
	}
}

// Insert inserts a meter readout into the SQL database.
func (s *SQL) Insert(readout Readout) {
	s.ensureInitialized()
	_, err := s.insertStatement.Exec(
		readout.Timestamp.Format("2006-01-02 15:04:05"),
		readout.Timestamp.Format("2006-01-02"),
		readout.Timestamp.Format("15:04:05"),
		readout.CurrentTarif(),
		readout.PowerReceived(),
		readout.PowerDelivered(),
		readout.GasReceived(2),
		readout.TotalPowerReceivedLowTarif(),
		readout.TotalPowerReceivedPeakTarif(),
		readout.TotalPowerDeliveredLowTarif(),
		readout.TotalPowerDeliveredPeakTarif(),
	)

	if err != nil {
		log.Fatal("Insert error:", err)
	}
}

func ReadoutDataFromReadout(r Readout) ReadoutData {
	return ReadoutData{
		timestamp:                    r.Timestamp,
		Timestamp:                    r.Timestamp.Format("2006-01-02 15:04:05"),
		Tarif:                        int(r.CurrentTarif()),
		PowerReceived:                r.PowerReceived(),
		PowerDelivered:               r.PowerDelivered(),
		GasReceived:                  r.GasReceived(2),
		TotalPowerDeliveredLowTarif:  r.TotalPowerDeliveredLowTarif(),
		TotalPowerDeliveredPeakTarif: r.TotalPowerDeliveredPeakTarif(),
		TotalPowerReceivedLowTarif:   r.TotalPowerReceivedLowTarif(),
		TotalPowerReceivedPeakTarif:  r.TotalPowerReceivedPeakTarif(),
	}
}

// ReadoutData can contain data as stored in the database.
type ReadoutData struct {
	timestamp                    time.Time
	Timestamp                    string
	Tarif                        int
	PowerReceived                float64
	PowerDelivered               float64
	GasReceived                  float64
	TotalPowerDeliveredLowTarif  float64
	TotalPowerDeliveredPeakTarif float64
	TotalPowerReceivedLowTarif   float64
	TotalPowerReceivedPeakTarif  float64
}

func (r *ReadoutData) getTimestamp() time.Time {
	if r.timestamp.Equal(time.Time{}) {
		t, err := time.Parse("2006-01-02 15:04:05", r.Timestamp)
		if err != nil {
			log.Println(err)
		}
		r.timestamp = t
	}

	return r.timestamp
}

func fieldsFromDataRetrievalOption(retrieve DataRetrievalOption) string {
	switch retrieve {
	case Gas:
		return "MIN(timestamp), MAX(gas_received)"
	case Power:
		return "timestamp, power_received, power_deliverd"
	case Totals:
		return "timestamp, total_power_received_low, total_power_received_peak, total_power_delivered_low, total_power_delivered_peak"
	case Gas + Power:
		return "timestamp, gas_received, power_received, power_deliverd"
	case Gas + Totals:
		return "timestamp, gas_received, total_power_received_low, total_power_received_peak, total_power_delivered_low, total_power_delivered_peak"
	case Power + Totals:
		return "timestamp, power_received, power_deliverd, total_power_received_low, total_power_received_peak, total_power_delivered_low, total_power_delivered_peak"
	}
	return "*"
}

func groupingFromDataRetrievalOption(retrieve DataRetrievalOption) string {
	switch retrieve {
	case Gas:
		return " GROUP BY gas_received"
	case Power:
		return ""
	case Totals:
		return ""
	case Gas + Power:
		return ""
	case Gas + Totals:
		return ""
	case Power + Totals:
		return ""
	}
	return ""
}

// GetRange retrieves a range of readout data from the database.
func (s *SQL) GetRange(start time.Time, end time.Time, retrieve DataRetrievalOption) ([]ReadoutData, error) {
	s.ensureInitialized()

	data := make([]ReadoutData, 0)
	sarg := start.Format("2006-01-02 15:04:05")
	earg := end.Format("2006-01-02 15:04:05")
	fields := fieldsFromDataRetrievalOption(retrieve)
	grouping := groupingFromDataRetrievalOption(retrieve)
	var q string = "SELECT " + fields + " FROM readouts WHERE timestamp >= ? AND timestamp <= ?" + grouping

	startTime := time.Now()
	log.Println("Running query: ", q, sarg, earg)
	rows, err := s.db.Query(q, sarg, earg)
	if err != nil {
		log.Println(err)
		return data, err
	}

	var ts, d, t string
	var id, tarif int
	var pRec, pDel, gRec, TPDL, TPDP, TPRL, TPRP float64
	log.Println("Retrieving data")
	for rows.Next() {
		switch retrieve {
		case Gas:
			rows.Scan(&ts, &gRec)
			break
		case Power:
			rows.Scan(&ts, &pRec, &pDel)
			break
		case Totals:
			rows.Scan(&ts, &TPRL, &TPRP, &TPDL, &TPDP)
			break
		case Gas + Power:
			rows.Scan(&ts, &gRec, &pRec, &pDel)
			break
		case Gas + Totals:
			rows.Scan(&ts, &gRec, &TPRL, &TPRP, &TPDL, &TPDP)
			break
		case Power + Totals:
			rows.Scan(&ts, &pRec, &pDel, &TPRL, &TPRP, &TPDL, &TPDP)
			break
		default:
			rows.Scan(&id, &ts, &d, &t, &tarif, &pRec, &pDel, &gRec, &TPRL, &TPRP, &TPDL, &TPDP)
			break
		}
		data = append(data, ReadoutData{
			Timestamp:                    ts,
			Tarif:                        tarif,
			PowerReceived:                pRec,
			PowerDelivered:               pDel,
			GasReceived:                  gRec,
			TotalPowerReceivedLowTarif:   TPRL,
			TotalPowerReceivedPeakTarif:  TPRP,
			TotalPowerDeliveredLowTarif:  TPDL,
			TotalPowerDeliveredPeakTarif: TPDP,
		})
	}

	log.Println("Data retrieved in ", time.Now().Sub(startTime))
	return data, nil
}

// GetAveragedRange retrieves a set of readouts within the given range and averages them over a given interval.
func (s *SQL) GetAveragedRange(start time.Time, end time.Time, interval time.Duration, retrieve DataRetrievalOption) ([]ReadoutData, error) {
	completeRange, err := s.GetRange(start, end, retrieve)
	if err != nil || interval == time.Second {
		return completeRange, err
	}

	startTime := time.Now()
	indexes := getRangeIndexes(completeRange, interval)

	averagedRanges := make([]ReadoutData, 0)
	for _, i := range indexes {
		if i.start == i.end {
			averagedRanges = append(averagedRanges, completeRange[i.start])
			continue
		}

		currRange := completeRange[i.start:i.end]
		currReadout := ReadoutData{
			Timestamp: currRange[0].Timestamp,
			Tarif:     currRange[0].Tarif,
		}
		for _, c := range currRange {
			currReadout.PowerReceived += c.PowerReceived
			currReadout.PowerDelivered += c.PowerDelivered
			currReadout.GasReceived += c.GasReceived
			currReadout.TotalPowerDeliveredLowTarif += c.TotalPowerDeliveredLowTarif
			currReadout.TotalPowerDeliveredPeakTarif += c.TotalPowerDeliveredPeakTarif
			currReadout.TotalPowerReceivedLowTarif += c.TotalPowerReceivedLowTarif
			currReadout.TotalPowerReceivedPeakTarif += c.TotalPowerReceivedPeakTarif
		}

		divisor := float64(len(currRange))
		currReadout.PowerReceived = math.Round(currReadout.PowerReceived*1000/divisor) / 1000
		currReadout.PowerDelivered = math.Round(currReadout.PowerDelivered*1000/divisor) / 1000
		currReadout.GasReceived = math.Round(currReadout.GasReceived*1000/divisor) / 1000
		currReadout.TotalPowerDeliveredLowTarif = math.Round(currReadout.TotalPowerDeliveredLowTarif*1000/divisor) / 1000
		currReadout.TotalPowerDeliveredPeakTarif = math.Round(currReadout.TotalPowerDeliveredPeakTarif*1000/divisor) / 1000
		currReadout.TotalPowerReceivedLowTarif = math.Round(currReadout.TotalPowerReceivedLowTarif*1000/divisor) / 1000
		currReadout.TotalPowerReceivedPeakTarif = math.Round(currReadout.TotalPowerReceivedPeakTarif*1000/divisor) / 1000

		averagedRanges = append(averagedRanges, currReadout)
	}

	log.Println("Done averaging in:", time.Now().Sub(startTime))
	return averagedRanges, nil
}

type rangeKeys struct {
	start, end int
}

func getRangeIndexes(readouts []ReadoutData, interval time.Duration) []rangeKeys {
	var keys []rangeKeys

	startKey := 0
	numReadouts := len(readouts)
	for i := 0; i < numReadouts; i++ {
		if i == startKey {
			continue
		}

		sr := readouts[startKey]
		cr := readouts[i]

		sTime := sr.getTimestamp()
		if sTime.Equal(time.Time{}) {
			startKey++
			continue
		}

		cTime := cr.getTimestamp()
		if cTime.Sub(sTime) < interval && i < numReadouts-1 {
			continue
		}

		if i == numReadouts-1 {
			i++
		}

		keys = append(keys, rangeKeys{startKey, i - 1})
		startKey = i
	}

	return keys
}

func panicOnError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
