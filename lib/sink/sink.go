package sink

import (
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
	DataChan   chan string
	BufferSize int
	Name       string
}

func NewSinkCore(name string, bufferSize int) *SinkCore {
	return &SinkCore{
		DataChan:   make(chan string, 10000),
		Name:       name,
		BufferSize: bufferSize,
	}
}

func (s *SinkCore) ReadFinished() {
	// Nothing to do
}

func (s *SinkCore) Close() {
	s.DataChan = nil
}

func (s *SinkCore) Data(data string) {
	s.DataChan <- data
}

func (s *SinkCore) EndOfData() {
	close(s.DataChan)
}
