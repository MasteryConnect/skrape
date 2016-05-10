package skrape

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	utils "github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
)

const (
	BufferSize = 104857600 // 100MB in bytes
)

func (p Parameters) Perform(chn chan string) { // Perform the export from MySQL RDBMS to CSV files
	elapsed := time.Now()
	file, _ := os.OpenFile("tables.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)

	defer func() {

		p.Table.Reset()
		log.Infof("Completed: %s\n", p.Table.Name)
		log.Infof("Duration: %s\n", time.Since(elapsed).String())
		file.WriteString(<-chn + "\n")
		file.Sync()
		file.Close()

		fmt.Println("AFTER CALL TO DONE")
	}() // read off the semiphore channel to allow a new goroutine to start

	args := p.Connection.Setup()
	args = p.AddTable(args)
	app := utils.GetBinary()
	cmd := exec.Command(app, args...)
	cmdReader, _ := cmd.StdoutPipe()
	cmdError, _ := cmd.StderrPipe()

	var wait sync.WaitGroup

	// Monitor errors thrown by exec.Cmd
	scannerErr := bufio.NewScanner(cmdError)
	go func() {
		for scannerErr.Scan() {
			err := scannerErr.Text()
			if err != "" {
				log.WithFields(log.Fields{
					"msg": "There was an error while running mysqldump",
				}).Fatal(err)
			}
		}
	}()

	wait.Add(1)
	go p.Table.Write(&wait)

	scanner := bufio.NewScanner(cmdReader)
	wait.Add(1)
	go func() {
		defer wait.Done()
		var txt string
		preambleLen := -1
		recordStartMark := "VALUES ("
		log.Warnf("Begin scanning for: %s", p.Table.Name)
		for scanner.Scan() {
			txt = scanner.Text()
			if txt[0:1] == "--" { // skip any comments
				continue
			}

			if preambleLen == -1 { // measure start of values to be added to the CSV
				preambleLen = strings.Index(txt, recordStartMark) + len(recordStartMark)
			}

			if len(txt) > preambleLen {
				parsed := txt[preambleLen : len(txt)-2]
				p.Table.Data <- fmt.Sprintf("%s\n", parsed)
			} else { // log out bad value strings and continue
				log.Warn(fmt.Sprintf("%s\n", txt))
				continue
			}
		}
		close(p.Table.Data)
	}()

	err := cmd.Start()
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}

	err = cmd.Wait()
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}
	wait.Wait()
	fmt.Println("NO LONGER WAITING")
}

func (p Parameters) AddTable(a []string) (args []string) {
	args = append(a, p.Table.Name)
	return
}