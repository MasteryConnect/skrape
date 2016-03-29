package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/codegangsta/cli"
	"golang.org/x/crypto/ssh/terminal"
)

var defaultFile string
var host string
var port string
var user string

func init() {
	log.SetHandler(text.New(os.Stderr))
}

func main() {
	app := cli.NewApp()
	app.Name = "skrape"
	app.Usage = "export MySQL RDBMS tables to csv files"
	app.Action = func(c *cli.Context) {
		setup()
	}
	app.Run(os.Args)
	cmd := exec.Command("mysqldump", "--version")
	outfile, err := os.Create("./out.txt")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer outfile.Close()

	cmd.Stdout = outfile
	if err != nil {
		log.Fatal(err.Error())
	}

	if err = cmd.Start(); err != nil {
		log.Fatal(err.Error())
	}

	cmd.Wait()
}

func setup() []string {
	var args []string
	fmt.Print("Enter Password: ")
	bytePwd, err := terminal.ReadPassword(syscall.Stdin)
	if err != nil {
		log.Fatal(err.Error())
	}
	args = append(args, fmt.Sprintf("--defaults-file=%s", MysqlDefaults(string(bytePwd))))
	args = append(args, fmt.Sprintf("--host %s", host))
	args = append(args, "--skip-opt")
	args = append(args, "--compact")
	args = append(args, "--no-create-db")
	args = append(args, "--no-create-info")
	args = append(args, "--quick")
	args = append(args, "--single-transaction")
	return args
}

func MysqlDefaults(pwd string) string {
	defaultFile := "/tmp/mysql-cnf.cnf"
	file, err := os.Create(defaultFile)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	_, err = w.WriteString("[client]\n")
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = w.WriteString(pwd)
	if err != nil {
		log.Fatal(err.Error())
	}

	w.Flush()

	return defaultFile
}
