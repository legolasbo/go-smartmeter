package smartmeter

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"regexp"
	"time"
)

var (
	dateTimeRegex = regexp.MustCompile(`(\d{4}-\d{2}-\d{2})? ?(\d{2}:\d{2}:\d{2})?`)
)

// Storage provides an abstraction for the storage backend.
type Storage interface {
	// Insert inserts a readout into the storage backend.
	Insert(readout Readout)
	// GetRange retrieves a set of readouts within the given range.
	GetRange(start time.Time, end time.Time) ([]readoutData, error)
	// GetAveragedRange retrieves a set of readouts within the given range and averages them over a given interval.
	GetAveragedRange(start time.Time, end time.Time, interval time.Duration) ([]readoutData, error)
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
		readout.Timestamp().Format("2006-01-02 15:04:05"),
		readout.Timestamp().Format("2006-01-02"),
		readout.Timestamp().Format("15:04:05"),
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

type readoutData struct {
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

func (r *readoutData) getTimestamp() time.Time {
	if r.timestamp.Equal(time.Time{}) {
		t, err := time.Parse("2006-01-02 15:04:05", r.Timestamp)
		if err != nil {
			log.Println(err)
		}
		r.timestamp = t
	}

	return r.timestamp
}

// GetRange retrieves a range of readout data from the database.
func (s *SQL) GetRange(start time.Time, end time.Time) ([]readoutData, error) {
	s.ensureInitialized()

	data := make([]readoutData, 0)
	sarg := start.Format("2006-01-02 15:04:05")
	earg := end.Format("2006-01-02 15:04:05")
	rows, err := s.db.Query(`SELECT * FROM readouts WHERE timestamp >= ? AND timestamp <= ?`, sarg, earg)
	if err != nil {
		log.Println(err)
		return data, err
	}

	var ts, d, t string
	var id, tarif int
	var pRec, pDel, gRec, TPDL, TPDP, TPRL, TPRP float64
	for rows.Next() {
		rows.Scan(&id, &ts, &d, &t, &tarif, &pRec, &pDel, &gRec, &TPRL, &TPRP, &TPDL, &TPDP)
		data = append(data, readoutData{
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

	return data, nil
}

// GetAveragedRange retrieves a set of readouts within the given range and averages them over a given interval.
func (s *SQL) GetAveragedRange(start time.Time, end time.Time, interval time.Duration) ([]readoutData, error) {
	completeRange, err := s.GetRange(start, end)
	if err != nil || interval == time.Second {
		return completeRange, err
	}

	indexes := getRangeIndexes(completeRange, interval)

	averagedRanges := make([]readoutData, 0)
	for _, i := range indexes {
		currRange := completeRange[i.start:i.end]
		currReadout := readoutData{
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

	return averagedRanges, nil
}

type rangeKeys struct {
	start, end int
}

func getRangeIndexes(readouts []readoutData, interval time.Duration) []rangeKeys {
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

// StringToTime converts a string to a valid time to be used in a range query.
func StringToTime(str string, loc *time.Location, defaultTime string) (time.Time, error) {
	f := "2006-01-02 15:04:05"
	match := dateTimeRegex.FindStringSubmatch(str)
	if match[1] != "" && match[2] != "" {
		return time.ParseInLocation(f, match[0], loc)
	}

	if match[1] != "" {
		str = fmt.Sprintf("%s %s", match[1], defaultTime)
		return time.ParseInLocation(f, str, loc)
	}

	if match[2] != "" {
		str = fmt.Sprintf("%s %s", time.Now().Format("2006-01-02"), match[2])
		return time.ParseInLocation(f, str, loc)
	}

	str = fmt.Sprintf("%s %s", time.Now().Format("2006-01-02"), defaultTime)
	return time.ParseInLocation(f, str, loc)
}
