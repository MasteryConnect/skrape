package sink

import (
	"bufio"
	"github.com/apex/log"
	"sync"
)

type Sink interface {
	Write(*sync.WaitGroup)
	ReadFinished()
	Close()
	Data(string)
	EndOfData()
}

type SinkCore struct {
	Buffer     *bufio.Writer
	DataChan   chan string
	BufferSize int
	Name       string
}

func NewSinkCore(buf *bufio.Writer, name string, bufferSize int) *SinkCore {
	return &SinkCore{
		Buffer:     buf,
		DataChan:   make(chan string, 10000),
		Name:       name,
		BufferSize: bufferSize,
	}
}

// Main writing function for each table.
// this function is responsible for writing
// the exported table to disk.
func (s *SinkCore) Write(wg *sync.WaitGroup) {
	defer func() {
		s.Buffer.Flush()
		wg.Done()
		log.Debug("Channel closed, table should be fully exported")
	}()

	for str := range s.DataChan {
		s.Buffer.WriteString(str)
		if s.Buffer.Available() <= s.BufferSize/10 {
			s.Buffer.Flush()
		}
	}
}

func (s *SinkCore) Close() {
	s.Buffer.Flush()
	s.Buffer = nil
	s.DataChan = nil
}

func (s *SinkCore) Data(data string) {
	s.DataChan <- data
}

func (s *SinkCore) EndOfData() {
	close(s.DataChan)
}
