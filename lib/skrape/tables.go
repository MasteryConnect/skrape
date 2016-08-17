package skrape

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/MasteryConnect/skrape/lib/skrape/skrapes3"
	"github.com/MasteryConnect/skrape/lib/utility"
	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
)

type Table struct {
	Name string
	Path string
}

type Schema struct {
	Fields []Field `json:"fields"`
}

type Paths struct {
	JsonPaths []string `json:"jsonpaths"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Null string `json:"null"`
}

func NewTable(path, name string) *Table {
	t := &Table{}
	t.Path = path
	t.Name = name
	return t
}

// Export a table schema to S3
func (e *Extract) Schema(t *Table) {
	db := e.Connect()
	schema := Schema{[]Field{}}
	paths := Paths{[]string{}}
	schemaname := t.Name + ".json"
	pathsname := t.Name + "paths.json"
	schemafile, _ := os.Create(t.Path + "/" + schemaname)
	pathsfile, _ := os.Create(t.Path + "/" + pathsname)

	defer db.Close()
	defer schemafile.Close()
	defer pathsfile.Close()

	query := fmt.Sprintf("select COLUMN_NAME as `Field`, COLUMN_TYPE as `Type`, IS_NULLABLE AS `Null` from information_schema.COLUMNS WHERE TABLE_NAME = '%s'", t.Name)

	rows, err := db.Query(query)
	if err != nil {
		log.WithField("error", err).Fatal("there was an error extracting the schema for:" + t.Name)
	}
	for rows.Next() {
		var f Field
		rows.Scan(&f.Name, &f.Type, &f.Null)
		paths.JsonPaths = append(paths.JsonPaths, fmt.Sprintf("$['%s']", f.Name))
		schema.Fields = append(schema.Fields, f)
	}

	encoder := json.NewEncoder(schemafile)
	encoder.Encode(schema)
	schemafile.Sync()

	encoder = json.NewEncoder(pathsfile)
	encoder.Encode(paths)
	pathsfile.Sync()

	skrapes3.S3Upload(schemafile, os.Getenv("S3_BUCKET"), fmt.Sprintf("%s/%s/schemas/%s", os.Getenv("S3_KEY"), skrapes3.S3DateKey(), schemaname))
	os.Remove(t.Path + "/" + schemaname)
	log.Info("Schema uploaded for: " + t.Name)

	skrapes3.S3Upload(pathsfile, os.Getenv("S3_BUCKET"), fmt.Sprintf("%s/%s/paths/%s", os.Getenv("S3_KEY"), skrapes3.S3DateKey(), pathsname))
	os.Remove(t.Path + "/" + pathsname)
	log.Info("JSONPaths file uploaded for: " + t.Name)
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
	if e.Connection.Match == true {
		e.Connection.Concurrency = c
	}
}
