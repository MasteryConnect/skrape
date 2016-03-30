package export

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"

	utils "github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
)

func Perform(host, user, port string) {
	args, filePath := Setup(host, user, port)
	cmd := exec.Command(utils.GetBinary(), args...)

	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Error creating StdoutPipe for Cmd: %v\n%v\n", os.Stderr, err)
	}

	scanner := bufio.NewScanner(cmdReader)
	go func() {
		for scanner.Scan() {
			fmt.Printf("docker build out | %s\n", scanner.Text())
		}
	}()
}
