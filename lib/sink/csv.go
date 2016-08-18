package sink

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/apex/log"
)

type CsvSink struct {
	*SinkCore

	Buffer   *bufio.Writer
	Path     string
	FileName string
	File     *os.File
}

func NewCsvSink(path, name string, bufferSize int) *CsvSink {
	file, err := os.Create(fmt.Sprintf("%s/%s.csv", path, name))
	if err != nil {
		log.WithField("error", err.Error()).Fatal("Could not create file for exporting")
	}
	buf := bufio.NewWriterSize(file, bufferSize)
	cs := &CsvSink{
		Path:     path,
		Buffer:   buf,
		FileName: name + ".csv",
		File:     file,
		SinkCore: NewSinkCore(name, bufferSize),
	}
	return cs
}

// Main writing function for each table.
// this function is responsible for writing
// the exported table to disk.
func (s *CsvSink) Write(wg *sync.WaitGroup) {
	defer func() {
		s.Buffer.Flush()
		wg.Done()
		log.Debug("Channel closed, table should be fully exported")
	}()

	for str := range s.DataChan {
		s.Buffer.WriteString(fmt.Sprintf("%s\n", str))
		if s.Buffer.Available() <= s.BufferSize/10 {
			s.Buffer.Flush()
		}
	}
}

func (s *CsvSink) Close() {
	s.SinkCore.Close()
	s.Buffer.Flush()
	s.Buffer = nil
	s.File.Close()
}
