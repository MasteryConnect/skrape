package export

import (
	"bufio"
	"fmt"
	"os"
	"syscall"

	"github.com/apex/log"
	"golang.org/x/crypto/ssh/terminal"
)

const DefaultFile = "/tmp/mysql-cnf.cnf"

type Parameters struct {
	Host     string
	User     string
	Port     string
	Database string
	Table    string
	All      bool
}

func NewParameters(host, user, port, db string) *Parameters { // Will setup to default for exporting all tables
	params := &Parameters{
		Host:     host,
		User:     user,
		Port:     port,
		Database: db,
		Table:    "",
		All:      true,
	}

	return params
}

func (p *Parameters) Setup() []string {
	if _, err := os.Stat(DefaultFile); err != nil {
		log.Info("defaults file is missing")
		p.MysqlDefaults()
	}
	var args []string
	args = append(args, fmt.Sprintf("--defaults-file=%s", DefaultFile))
	args = append(args, fmt.Sprintf("--host=%s", p.Host))
	args = append(args, fmt.Sprintf("--user=%s", p.User))
	if p.Port != "" {
		args = append(args, fmt.Sprintf("--port=%s", p.Port))
	}
	args = append(args, "--skip-opt")
	args = append(args, "--compact")
	args = append(args, "--no-create-db")
	args = append(args, "--no-create-info")
	args = append(args, "--quick")
	args = append(args, "--single-transaction")
	args = append(args, p.Database)
	if p.All == false {
		args = append(args, p.Table)
	}
	return args
}

func (p *Parameters) MysqlDefaults() string {
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
