package skrape

import (
	"bufio"
	"database/sql"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/MasteryConnect/skrape/lib/config"
	utils "github.com/MasteryConnect/skrape/lib/mysqlutils"
	sinks "github.com/MasteryConnect/skrape/lib/sink"
	"github.com/apex/log"
)

const (
	BufferSize       = 209715200 // 200MB in bytes
	KinesisBatchSize = 1000      // Records per PutRecord call
)

type Extract struct {
	SinkType string
	Cfg      config.Config
}

func NewExtract(sinkType string, c config.Config) *Extract {
	return &Extract{sinkType, c}
}

func (e *Extract) Perform(semaphore chan bool, name string) { // Perform the export from MySQL RDBMS to CSV files
	elapsed := time.Now()
	// Source
	table := NewTable(e.Destination(), name)
	// Sink
	var sink sinks.Sink
	switch e.SinkType {
	case "csv":
		sink = sinks.NewCsvSink(e.Destination(), name, BufferSize)
	case "kinesis":
		sink = sinks.NewKinesisSink(e.Destination(), name, KinesisBatchSize, e.Cfg)
	default:
		sink = sinks.NewS3Sink(e.Destination(), name, BufferSize, e.Cfg)
	}
	log.Debug("Inside Perform Function")

	defer func() {
		log.WithFields(log.Fields{
			"TableName": name,
			"Duration":  time.Since(elapsed).String(),
		}).Info("Completed")
		<-semaphore
		log.WithField("TableName", name).Debug("Read off the channel, opened up a spot for a new routine")
	}() // read off the semiphore channel to allow a new goroutine to start

	args := e.Setup()
	args = table.AddTable(args)
	app := utils.GetBinary() // mysqldump binary
	cmd := exec.Command(app, args...)
	cmdReader, _ := cmd.StdoutPipe()
	cmdError, _ := cmd.StderrPipe()

	var wait sync.WaitGroup

	// Monitor errors thrown by exec.Cmd
	scannerErr := bufio.NewScanner(cmdError)
	wait.Add(1)
	go func() {
		defer wait.Done()
		for scannerErr.Scan() {
			err := scannerErr.Text()
			if err != "" {
				log.WithFields(log.Fields{
					"msg": "There was an error while running mysqldump",
				}).Fatal(err)
			}
		}
	}()

	// Here we start the writer and set it up to wait for the channel to receive
	// data from the reader below
	wait.Add(1)
	go sink.Write(&wait)

	// This will increase the buffer size for bufio.Scanner to allow for
	// token sizes (lines) up to 1MB in size.
	buf := make([]byte, 0, 64*1024)
	scanner := bufio.NewScanner(cmdReader)
	scanner.Buffer(buf, 1024*1024)
	wait.Add(1)

	// Here we start the reader to read from the command output line by line
	// each line is parsed to become only a csv string and is then pushed onto a channel
	go func() {
		defer func() {
			wait.Done()
			log.Debug("Completed read routine")
		}()
		var txt string
		preambleLen := -1
		recordStartMark := "VALUES ("
		log.Infof("Begin scanning for: %s", table.Name)
		for scanner.Scan() {
			txt = scanner.Text()
			if txt[0:6] != "INSERT" { // skip any comments or other gibberish
				continue
			}

			if preambleLen == -1 { // measure start of values to be added to the CSV
				preambleLen = strings.Index(txt, recordStartMark) + len(recordStartMark)
			}

			if len(txt) > preambleLen {
				parsed := txt[preambleLen : len(txt)-2] // Drop off ); at end of line
				sink.Data(parsed)                       // add parsed line to the channel
			} else { // log out bad value strings and continue
				log.Debug("BAD JOO JOO found in extraction")
				log.Warn(fmt.Sprintf("%s\n", txt))
				continue
			}
		}
		if scanner.Err() != nil {
			log.WithError(scanner.Err()).Fatal("There was an error while reading the mysqldump output")
		}
		sink.EndOfData() // closes the channel once the read operation is completed
		log.WithField("TableName", name).Debug("Just closed the table data channel")
	}()

	// Start the mysqldump command
	log.WithField("TableName", name).Debug("Starting mysqldump")
	err := cmd.Start()
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}

	// Wait for mysqldump to complete
	log.WithField("TableName", name).Debug("Waiting for export to complete")

	// Waiting for all waitgroups from the reader and writer to finish up
	// this line must be before cmd.Wait() otherwise you may get an error
	// for trying to read from cmd after the shell command has completed
	wait.Wait()

	err = cmd.Wait()
	log.Debugf("mysqldump %s", cmd.ProcessState.String())
	log.Debugf("mysqldump completed for %s", name)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.WithFields(log.Fields{
			"file": file,
			"line": line,
		}).Error(err.Error())
	}
	sink.ReadFinished()
	sink.Close()
}

// Abstraction functions for disconnecting
// Connection from the skrape package
// TODO create interfaces for Connection
func (e *Extract) Connect() *sql.DB {
	return e.Cfg.GetConn().Connect()
}

func (e *Extract) Destination() string {
	path := e.Cfg.GetConn().Destination
	var p string
	if path[len(path)-1:] == "/" { // store path without trailing slash for consistency
		p = path[:len(path)-1]
	} else {
		p = path
	}
	return p
}

func (e *Extract) Setup() []string {
	return e.Cfg.GetConn().Setup()
}

func (e *Extract) Concurrency() int {
	return e.Cfg.GetConn().Concurrency
}

func (e *Extract) Database() string {
	return e.Cfg.GetConn().Database
}
