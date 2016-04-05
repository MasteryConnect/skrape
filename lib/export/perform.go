package export

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	// "regexp"

	utils "github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
)

func (p *Parameters) Perform(c chan bool) { // Perform the export from MySQL RDBMS to CSV files
	args := p.Setup()

	app := utils.GetBinary()
	cmd := exec.Command(app, args...)

	cmdReader, err := cmd.StdoutPipe()
	cmdError, err := cmd.StderrPipe()
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Errorf("Error creating StdoutPipe for Cmd: %v\n%", err)
	}

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

	scanner := bufio.NewScanner(cmdReader)
	file, _ := os.Create(fmt.Sprintf("test/%s.csv", p.Table))
	buf := bufio.NewWriterSize(file, 524288000)
	go func(s *bufio.Scanner, buf *bufio.Writer, file *os.File, c chan bool) {
		defer file.Close()
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
				_, err = buf.WriteString(fmt.Sprintf("%s\n", txt[preambleLen:len(txt)-2]))
				if err != nil {
					log.Warn(err.Error())
				}
			} else { // log out bad value strings and continue
				log.Warn(fmt.Sprintf("%s\n", txt))
				continue
			}
			if buf.Available() < 2097152 {
				buf.Flush()
			}
		}
		if buf.Buffered() > 0 { // Make sure buffer is flushed in case its overall size is less than 2MB
			log.Infof("Buffer Amount: %v", buf.Buffered())
			err = buf.Flush()
			if err != nil {
				log.Error(err.Error())
			}
		}
		c <- true
	}(scanner, buf, file, c)

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
}
