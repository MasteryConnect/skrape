package skrape

import (
	"bufio"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	utils "github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
)

const dumpDir = "/tmp/data"

var mutex = &sync.Mutex{}

type LO struct {
	Limit    int
	Offset   int
	Iterator int
}

type Priority struct {
	Names     []string
	BatchSize int
}

func (p *Priority) NewLO() *LO {
	return &LO{p.BatchSize, 0, 0}
}

func NewPriority(n []string, b int) *Priority {
	p := &Priority{}
	p.Names = n
	p.BatchSize = b
	return p
}

func (lo *LO) Runner(a []string, name string, w sync.WaitGroup) {
	defer w.Done()
	app := utils.GetBinary() // get the command (inlcuding path) to the binary of mysqldump

	// THINGS THAT NEED TO HAPPEN
	// 1. setup the mysqldump command to run
	// 2. setup an instance of table to coincide with the command
	// 3. write skraped data via the Table buffer
	// 4. repeat the loop until the counter doesn't equal the limit
	// set label for loop point
begin:
	table := lo.NewBatchTable(dumpDir, name)
	counter := 0 // counter for monitoring number of results scanned from mysqldump
	mutex.Lock()
	args := lo.UpdateArgs(a)
	mutex.Unlock()

	cmd := exec.Command(app, args...) // prepare command for execution
	cmdReader, _ := cmd.StdoutPipe()
	cmdError, _ := cmd.StderrPipe()

	scannerErr := bufio.NewScanner(cmdError) // Monitor errors thrown by exec.Cmd
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

	var wait sync.WaitGroup

	wait.Add(1)
	go table.Write(&wait)

	scanner := bufio.NewScanner(cmdReader)
	wait.Add(1)
	go func() {
		defer wait.Done()
		var txt string
		preambleLen := -1
		recordStartMark := "VALUES ("
		for scanner.Scan() {
			txt = scanner.Text()
			if txt[0:1] == "--" { // skip any comments
				continue
			}

			if preambleLen == -1 { // measure start of values to be added to the CSV
				preambleLen = strings.Index(txt, recordStartMark) + len(recordStartMark)
			}

			if len(txt) > preambleLen {
				counter++
				parsed := txt[preambleLen : len(txt)-2]
				table.Data <- fmt.Sprintf("%s\n", parsed)
			} else { // log out bad value strings and continue
				log.Warn(fmt.Sprintf("%s\n", txt))
				continue
			}
		}
		close(table.Data)
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

	if counter == lo.Limit {
		goto begin
	}
}

func (p Parameters) Priority() {
	var wait sync.WaitGroup
	connect := p.Connection
	args := connect.Setup() // retrieve arguments for mysqldump

	// Itterate over tables marked as priority
	for _, name := range p.Connection.Priority.Names {
		lo := connect.Priority.NewLO() // get a new limit offset struct
		p.Table.Name = name            // current table name for iteration
		args = p.AddTable(args)        // add the table name for this iteration to the mysqldump arguments

		// INSERT GO ROUTINE HERE
		// need to create a goroutine to start creating multiple instances of the mysqldump
		// command to run asynchronously, dumping output into the /tmp directory
		for i := 0; i < 20; i++ {
			wait.Add(1)
			go lo.Runner(args, name, wait)
		}
		wait.Wait()
	}
}

func (lo *LO) UpdateArgs(a []string) (args []string) {
	args = a
	for i, val := range args {
		if val[:7] == "--where" {
			args[i] = lo.ChangeWhere(val)

			break
		}
	}
	return
}

func (lo *LO) ChangeWhere(str string) (s string) {
	regex, _ := regexp.Compile("offset\\s(?P<offset>\\d*)")
	index := regex.FindAllStringSubmatchIndex(str, -1)[0][2]

	s = str[:index] + strconv.Itoa(lo.Offset)
	lo.Update()
	return
}

func (lo *LO) Update() {
	lo.Offset += lo.Limit
}
