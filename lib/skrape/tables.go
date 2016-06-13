package skrape

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/MasteryConnect/skrape/lib/utility"
	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	_ "github.com/go-sql-driver/mysql"
)

const PwdByteLength = 1024

var db *sql.DB

type Table struct {
	Name     string
	Path     string
	FileName string
	File     *os.File
	Buffer   *bufio.Writer
	Data     chan string
}

type Schema struct {
	Fields []Field
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Null bool   `json:"null"`
}

func NewTable(path, name string) *Table {
	var p string
	if path[len(path)-1:] == "/" { // store path without trailing slash for consistency
		p = path[:len(path)-1]
	} else {
		p = path
	}
	file, err := os.Create(fmt.Sprintf("%s/%s.csv", p, name))
	if err != nil {
		log.WithField("error", err.Error()).Fatal("Could not create file for exporting")
	}
	buf := bufio.NewWriterSize(file, BufferSize)
	t := &Table{}
	t.Path = p
	t.FileName = name + ".csv"
	t.Name = name
	t.Data = make(chan string, 10000)
	t.File = file
	t.Buffer = buf
	return t
}

// Main writing function for each table.
// this function is responsible for writing
// the exported table to disk.
func (t *Table) Write(wg *sync.WaitGroup) {
	defer func() {
		t.Buffer.Flush()
		wg.Done()
		log.Debug("Channel closed, table should be fully exported")
	}()

	for str := range t.Data {
		t.Buffer.WriteString(str)
		if t.Buffer.Available() <= BufferSize*.1 {
			t.Buffer.Flush()
		}
	}
}

func (t *Table) Schema() {

}

func (t *Table) Upload() {
	year := time.Now().Format("2006")
	month := time.Now().Format("01")
	day := time.Now().Format("02")
	file, _ := os.Open(fmt.Sprintf("%s/%s", t.Path, t.FileName))
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		_, err := io.Copy(gw, file)
		if err != nil {
			log.WithField("error", err).Fatal("There was an error gzipping a file")
		}
		file.Close()
		gw.Close()
		writer.Close()
	}()
	uploader := s3manager.NewUploader(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))}))
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(os.Getenv("S3_BUCKET")),
		Key:    aws.String(fmt.Sprintf("%s/%s/%s/%s/%s", os.Getenv("S3_KEY"), year, month, day, t.Name+".csv.gz")),
	})
	if err != nil {
		log.WithField("error", err).Fatal("Failed to upload file.")
	}
	os.Remove(t.Path + "/" + t.FileName)
	log.WithField("location", result.Location).Info("Successfully uploaded to")
}

// For decreasing the memory foot print as
// go routines complete their task
func (t *Table) Reset() {
	t.Buffer.Flush()
	t.Buffer = nil
	t.File = nil
	t.Data = nil
}

func (t *Table) AddTable(a []string) (args []string) {
	args = append(a, t.Name)
	return
}

func (c *Connection) TableHandler(priority, exclude []string) { // grab all tables from the database
	db := c.connect()
	tableNames := c.readTables(db)

	// if priority is flagged, move the tables to the front of the list
	if len(priority) > 0 {
		tableNames = utility.MoveToFrontOfSlice(priority, tableNames)
	}
	if len(exclude) > 0 {
		tableNames = utility.SlcDelFrmSlc(exclude, tableNames)
	}

	semaphore := make(chan bool, c.Concurrency)
	for _, name := range tableNames {
		semaphore <- true
		go c.Perform(semaphore, name)
	}
	log.Debug("At this point all tables have been issued a request to export. Waiting for exports to finish") // debugging

	for i := 0; i < cap(semaphore); i++ {
		log.Debugf("Semaphore Capacity: %d", i)
		semaphore <- true
	}

	log.Debug("Looped all tables, should be exiting") // debugging
}

func (c *Connection) connect() *sql.DB {
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
	return db
}

func (c *Connection) readTables(db *sql.DB) []string {
	var tableNames []string

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
	return tableNames
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
