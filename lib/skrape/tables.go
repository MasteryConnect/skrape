package skrape

import (
	"fmt"
	"runtime"

	"github.com/MasteryConnect/skrape/lib/utility"
	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
)

type Table struct {
	Name string
	Path string
}

func NewTable(path, name string) *Table {
	t := &Table{}
	t.Path = path
	t.Name = name
	return t
}

// Add the database table to the argument list for Mysqldump
func (t *Table) AddTable(a []string) (args []string) {
	args = append(a, t.Name)
	return
}

// Handles the control flow of exporting all tables from a database.
// This funciton institutes a semaphore pattern for controlling
// how many tables are exporting at once.
func (e *Extract) TableHandler(priority, exclude []string) { // grab all tables from the database
	tableNames := e.ReadTables()

	// if priority is flagged, move the tables to the front of the list
	if len(priority) > 0 {
		tableNames = utility.MoveToFrontOfSlice(priority, tableNames)
	}
	if len(exclude) > 0 {
		tableNames = utility.SlcDelFrmSlc(exclude, tableNames)
	}

	semaphore := make(chan bool, e.Concurrency())
	for _, name := range tableNames {
		semaphore <- true
		go e.Perform(semaphore, name)
	}
	log.Debug("At this point all tables have been issued a request to export. Waiting for exports to finish") // debugging

	// Push max buffer onto channel to make sure all
	// processes have completed
	for i := 0; i < cap(semaphore); i++ {
		log.Debugf("Semaphore Capacity: %d", i)
		semaphore <- true
	}

	log.Debug("Looped all tables, should be exiting")
}

// Pull all the table names from the database provided excluding views.
// Returns a slice of strings containing the table names.
func (e *Extract) ReadTables() []string {
	db := e.Connect()
	defer db.Close()
	var tableNames []string

	// lookup tables on database
	log.Info("Looking up tables")
	rows, err := db.Query(fmt.Sprintf("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE <> 'VIEW'", e.Database()))
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Fatal(err.Error())
	}

	// parse results from query of tables on database
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.WithFields(log.Fields{
				"file": file,
				"line": line,
			}).Fatal(err.Error())
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Fatal(err.Error())
	}
	e.UpdateConcurrency(len(tableNames))
	return tableNames
}

func (e *Extract) UpdateConcurrency(c int) {
	if e.Cfg.GetConn().Match == true {
		e.Cfg.GetConn().Concurrency = c
	}
}
