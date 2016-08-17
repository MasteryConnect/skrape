package csv

import (
	"bufio"
	"fmt"
	"os"

	"github.com/MasteryConnect/skrape/lib/sink"
	"github.com/apex/log"
)

type CsvSink struct {
	*sink.SinkCore

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
		SinkCore: sink.NewSinkCore(buf),
	}
	return cs
}

func (s *CsvSink) Close() {
	s.SinkCore.Close()
	s.File.Close()
}
