package setup

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"syscall"

	"github.com/apex/log"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	DefaultFile   = "/tmp/mysql-cnf.cnf"
	Limit         = 5000
	PwdByteLength = 1024
)

type Connection struct {
	Host        string
	User        string
	Port        string
	Database    string
	Destination string
	Concurrency int
	Match       bool
}

func NewConnection(host, user, port, db, dest string, conc int, match bool) (c *Connection) { // Will setup to default for exporting all tables
	c = &Connection{
		Host:        host,
		User:        user,
		Port:        port,
		Database:    db,
		Destination: dest,
		Concurrency: conc,
		Match:       match,
	}
	return
}

// Create connection to the database using the
// credentials from Connection
func (c *Connection) Connect() *sql.DB {
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

func (c *Connection) Missing() (a bool) {
	if c.Host != "" && c.User != "" && c.Database != "" {
		a = true
	}
	return
}

func (c *Connection) Setup() []string {
	if _, err := os.Stat(DefaultFile); err != nil {
		log.Info("defaults file is missing")
		MysqlDefaults()
	}
	var args []string
	args = append(args, fmt.Sprintf("--defaults-file=%s", DefaultFile))
	args = append(args, fmt.Sprintf("--host=%s", c.Host))
	args = append(args, fmt.Sprintf("--user=%s", c.User))
	if c.Port != "" {
		args = append(args, fmt.Sprintf("--port=%s", c.Port))
	}
	args = append(args, "--skip-opt")
	args = append(args, "--compact")
	args = append(args, "--no-create-db")
	args = append(args, "--no-create-info")
	args = append(args, "--quick")
	args = append(args, "--single-transaction")
	args = append(args, "--default-character-set=utf8")
	args = append(args, c.Database)

	return args
}

// Block stdout from printing to the screen
// so the database password can be read and
// stored into a tempfile without being
// displayed on the screen. Will restore
// stdout after the password is read.
func MysqlDefaults() string {
	fmt.Print("Enter Password: ")
	bytePwd, err := terminal.ReadPassword(syscall.Stdin)
	fmt.Println("")
	if err != nil {
		log.Fatal(err.Error())
	}
	file, err := os.Create(DefaultFile)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	_, err = w.WriteString(fmt.Sprintf("[client]\npassword=%s", string(bytePwd)))
	if err != nil {
		log.Fatal(err.Error())
	}

	w.Flush()
	file.Chmod(0600)

	return DefaultFile
}

// SUPPORTING FUNCTIONS

// Format the DSN string for connecting to the MySQL database
func (c *Connection) formatDsn(pwd string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", c.User, pwd, c.Host, c.Port, c.Database)
}

// Retrieves the password from the temp file
// that is created when the app first starts
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
