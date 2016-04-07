package export

import (
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
)

const PwdByteLength = 1024

var db *sql.DB

// Tables is a mash up of slices of strings
// -- Original contains all of the table names found in a database
// -- FirstHalf contains half of all the tablenames
// -- SecondHalf is simply the latter half of the table names plus any remainders
type Tables struct {
	Original []string
	Halves   [][]string
}

func NewTables(names []string) (t Tables) {
	t = Tables{}
	t.Original = names
	h := len(names) / 2
	r := len(names) % 2
	fh := h
	sh := fh - 1 + r
	t.Halves = append(t.Halves, names[0:fh])
	t.Halves = append(t.Halves, names[sh:])
	return
}

func (p *Parameters) TableLookUp() {
	var tableNames []string

	// retrieve password from default file
	pwd := getPwd()
	dsn := p.formatDsn(pwd)

	// create db connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}

	// check db connection
	err = db.Ping()
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Fatal(err.Error())
	}

	// lookup tables on database
	rows, err := db.Query("show tables")
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

	// split tables evenly-ish into two slices
	// tables := NewTables(tableNames)

	// loop over tables updating p struct Table (use goroutines to handle splits)
	// and calling p.Perform()

	fmt.Println(tableNames)
	var wg sync.WaitGroup
	for _, name := range tableNames {
		wg.Add(1)
		perf := &Parameters{p.Connection, name, true}
		go perf.Perform(wg)
	}
	wg.Wait()

}

// func (p *Parameters) loopTables(tables []string, c chan bool) {
// 	for _, name := range tables {
// 		ch := make(chan bool)
// 		p.All = false
// 		p.Table = name
// 		log.Infof("Exporting: %s", p.Table)
// 		go p.Perform(ch)
// 	}
// 	c <- true
// }

func (p *Parameters) formatDsn(pwd string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", p.Connection.User, pwd, p.Connection.Host, p.Connection.Port, p.Connection.Database)
}

func getPwd() string {
	cnt := make([]byte, PwdByteLength)
	file, err := os.Open(DefaultFile)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}

	file.Seek(18, 0)
	i, err := file.Read(cnt)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}

	return string(cnt[:i])
}
