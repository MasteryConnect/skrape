package export

import (
	"bufio"
	"fmt"
	"os"
	"syscall"

	"github.com/apex/log"
	"golang.org/x/crypto/ssh/terminal"
)

var defaultFile string
var mysqlDumpPath string

func Setup(host, user, port string) ([]string, string) {
	var args []string
	fmt.Print("Enter Password: ")
	bytePwd, err := terminal.ReadPassword(syscall.Stdin)
	fmt.Println("")
	if err != nil {
		log.Fatal(err.Error())
	}
	args = append(args, fmt.Sprintf("--defaults-file=%s", mysqlDefaults(string(bytePwd))))
	args = append(args, fmt.Sprintf("--host %s", host))
	if port != "" {
		args = append(args, fmt.Sprintf("--port %s", port))
	}
	args = append(args, "--skip-opt")
	args = append(args, "--compact")
	args = append(args, "--no-create-db")
	args = append(args, "--no-create-info")
	args = append(args, "--quick")
	args = append(args, "--single-transaction")
	return args, defaultFile
}

func mysqlDefaults(pwd string) string {
	defaultFile = "/tmp/mysql-cnf.cnf"
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
	file.Chmod(0600)

	return defaultFile
}
