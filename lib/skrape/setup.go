package skrape

import (
	"bufio"
	"fmt"
	"os"
	"syscall"

	"github.com/apex/log"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	DefaultFile = "/tmp/mysql-cnf.cnf"
	Limit       = 5000
)

// type Parameters struct {
// 	Connection *Connection
// 	Table      Table
// }

type Connection struct {
	Host        string
	User        string
	Port        string
	Database    string
	Destination string
	Concurrency int
}

func NewConnection(host, user, port, db, dest string, conc int) (c *Connection) { // Will setup to default for exporting all tables
	c = &Connection{
		Host:        host,
		User:        user,
		Port:        port,
		Database:    db,
		Destination: dest,
		Concurrency: conc,
	}
	return
}

// func NewParameters(c *Connection, table Table) (p Parameters) {
// 	p = Parameters{
// 		Connection: c,
// 		Table:      table,
// 	}
// 	return
// }

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
