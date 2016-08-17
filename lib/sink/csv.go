package sink

import (
	"bufio"
	"fmt"
	"os"

	"github.com/apex/log"
)

type CsvSink struct {
	*SinkCore

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
		FileName: name + ".csv",
		File:     file,
		SinkCore: NewSinkCore(buf, name, bufferSize),
	}
	return cs
}

func (s *CsvSink) ReadFinished() {
	// Nothing to do
}

func (s *CsvSink) Close() {
	s.SinkCore.Close()
	s.File.Close()
}
