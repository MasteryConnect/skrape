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

const (
	BufferSize = 104857600 // 100MB in bytes
)

type Table struct {
	Name   string
	File   *os.File
	Buffer *bufio.Writer
	Data   chan string
}

func NewTable(path, name string) Table {
	var p string
	if path[len(path)-1:] == "/" { // store path without trailing slash for consistency
		p = path[:len(path)-1]
	} else {
		p = path
	}
	file, _ := os.Create(fmt.Sprintf("%s/%s.csv", p, name))
	buf := bufio.NewWriterSize(file, BufferSize)
	t := Table{}
	t.Name = name
	t.Data = make(chan string, 10000)
	t.File = file
	t.Buffer = buf
	return t
}

func (table Table) Write(wg *sync.WaitGroup) {
	defer wg.Done()
	for str := range table.Data {
		table.Buffer.WriteString(str)
		if table.Buffer.Available() <= BufferSize*.1 {
			table.Buffer.Flush()
		}
	}
}

func (table *Table) Reset() {
	table.Buffer.Flush()
	table.Buffer = nil
	table.File.Close()
	table.File = nil
	table.Data = nil
}

func (p Parameters) Perform(chn chan bool) { // Perform the export from MySQL RDBMS to CSV files
	defer func() { p.Table.Reset() }()
	defer func() { <-chn; log.Info("Start New Routine") }() // read off the semiphore channel to allow a new goroutine to start
	args := p.Connection.Setup()
	args = p.AddTable(args)
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

func (p Parameters) AddTable(a []string) (args []string) {
	args = append(a, p.Table.Name)
	return
}
