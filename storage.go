package smartmeter

import (
	"database/sql"
	"log"
	"time"
)

// Storage provides an abstraction for the storage backend.
type Storage interface {
	// Insert inserts a readout into the storage backend.
	Insert(readout Readout)
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
		readout.Timestamp().Format("2016-01-02"),
		readout.Timestamp().Format("15:04:05"),
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

func panicOnError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
