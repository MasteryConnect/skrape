package skrape

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/MasteryConnect/skrape/lib/utility"
	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
)

const PwdByteLength = 1024

var db *sql.DB

type Table struct {
	Name   string
	File   *os.File
	Buffer *bufio.Writer
	Data   chan string
}

func NewTable(path, name string) Table {
	var p string
	if path[len(path)-1:] == "/" { // store path without trailing slash for consistency
		p = path[:len(path)-1]
	} else {
		p = path
	}
	file, _ := os.Create(fmt.Sprintf("%s/%s.csv", p, name))
	buf := bufio.NewWriterSize(file, BufferSize)
	t := Table{}
	t.Name = name
	t.Data = make(chan string, 10000)
	t.File = file
	t.Buffer = buf
	return t
}

func (lo *LO) NewBatchTable(path, name string) Table {
	var p string
	var mutex sync.Mutex
	mutex.Lock()
	if path[len(path)-1:] == "/" { // store path without trailing slash for consistency
		p = path[:len(path)-1]
	} else {
		p = path
	}
	file, _ := os.Create(fmt.Sprintf("%s/%s_%v.csv", p, name, lo.Iterator))
	lo.Iterator++
	buf := bufio.NewWriterSize(file, BufferSize)
	t := Table{}
	t.Name = name
	t.Data = make(chan string)
	t.File = file
	t.Buffer = buf
	mutex.Unlock()
	return t
}

// Main writing function for each table.
// this function is responsible for writing
// the exported table to disk.
func (table Table) Write(wg *sync.WaitGroup) {
	defer func() { table.Buffer.Flush() }()
	defer wg.Done()
	for str := range table.Data {
		table.Buffer.WriteString(str)
		if table.Buffer.Available() <= BufferSize*.1 {
			table.Buffer.Flush()
		}
	}
	table.Buffer.Flush()
}

func (table *Table) Reset() {
	table.Buffer.Flush()
	table.Buffer = nil
	table.File.Close()
	table.File = nil
	table.Data = nil
}

func (c *Connection) TableLookUp(priority, exclude []string) { // grab all tables from the database
	var tableNames []string

	// retrieve password from default file
	pwd := getPwd()
	dsn := c.formatDsn(pwd)

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
	log.Info("Looking up tables")
	rows, err := db.Query(fmt.Sprintf("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE <> 'VIEW'", c.Database))
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

	// if priority is flagged, move the tables to the front of the list
	if len(priority) > 0 {
		tableNames = utility.MoveToFrontOfSlice(priority, tableNames)
	}
	if len(exclude) > 0 {
		tableNames = utility.SlcDelFrmSlc(exclude, tableNames)
	}

	fmt.Printf("LENGTH OF TABLES: %v\n", len(tableNames))
	chn := make(chan string, c.Concurrency)
	f, _ := os.OpenFile("found_tables.csv", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0660)
	defer f.Close()

	for _, name := range tableNames {
		f.WriteString(name + "\n")
		f.Sync()
		table := NewTable(c.Destination, name)
		params := NewParameters(c, table)
		chn <- params.Table.Name
		go params.Perform(chn)
	}
	fmt.Println("At this point all tables have been issued a request to export. Waiting for exports to finish")
	for i := 0; i < cap(chn); i++ {
		chn <- ""
		log.Infof("Current Channel Capacity: %v\n", i)
	}
	log.Warn("Looped all tables, should be exiting")
}

// SUPPORTING FUNCTIONS
func (c *Connection) formatDsn(pwd string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", c.User, pwd, c.Host, c.Port, c.Database)
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
