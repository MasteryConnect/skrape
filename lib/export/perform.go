package export

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"

	utils "github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
)

type Table struct {
	Name   string
	File   *os.File
	Buffer *bufio.Writer
	Data   chan string
}

func (p Parameters) NewTable(size int) *Table {
	var path string
	if p.Connection.Destination[len(p.Connection.Destination)-1:] == "/" { // store path without trailing slash for consistency
		path = p.Connection.Destination[:len(p.Connection.Destination)-1]
	} else {
		path = p.Connection.Destination
	}
	file, _ := os.Create(fmt.Sprintf("%s/%s.csv", path, p.Table))
	buf := bufio.NewWriter(file)
	t := &Table{}
	t.Name = p.Table
	t.Data = make(chan string, 500)
	t.File = file
	t.Buffer = buf
	return t
}

func (table *Table) Write(wg sync.WaitGroup) {
	defer table.File.Close()
	defer wg.Done()
	for str := range table.Data {
		table.Buffer.WriteString(str)
		table.Buffer.Flush()
	}
}

func (p *Parameters) Perform(wg sync.WaitGroup) { // Perform the export from MySQL RDBMS to CSV files
	defer wg.Done()
	args := p.Setup()
	app := utils.GetBinary()
	cmd := exec.Command(app, args...)
	cmdReader, err := cmd.StdoutPipe()
	cmdError, err := cmd.StderrPipe()

	var wait sync.WaitGroup

	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Errorf("Error creating StdoutPipe for Cmd: %v\n%", err)
	}

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

	table := p.NewTable(500)

	wait.Add(1)
	go table.Write(wait)

	scanner := bufio.NewScanner(cmdReader)
	wait.Add(1)
	go func(s *bufio.Scanner, table *Table) {
		defer wait.Done()
		var txt string
		preambleLen := -1
		recordStartMark := "VALUES ("
		log.Warnf("Begin scanning for: %s", p.Table)
		for s.Scan() {
			txt = s.Text()
			if txt[0:1] == "--" { // skip any comments
				continue
			}

			if preambleLen == -1 { // measure start of values to be added to the CSV
				preambleLen = strings.Index(txt, recordStartMark) + len(recordStartMark)
			}

			if len(txt) > preambleLen {
				parsed := txt[preambleLen : len(txt)-2]
				table.Data <- fmt.Sprintf("%s\n", parsed)
			} else { // log out bad value strings and continue
				log.Warn(fmt.Sprintf("%s\n", txt))
				continue
			}
		}
		close(table.Data)
	}(scanner, table)

	err = cmd.Start()
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
}
