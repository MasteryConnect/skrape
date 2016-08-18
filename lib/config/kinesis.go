package config

import (
	"strings"
)

type kinesis struct {
	Endpoint   string
	StreamName string
}

const REPLACE = "{TABLE_NAME}"

func (this *config) GetKinesis() *kinesis {
	return this.Kinesis
}

func NewKinesis(endpoint, name string) *kinesis {
	return &kinesis{
		Endpoint:   endpoint,
		StreamName: name,
	}
}

func (this *kinesis) GetEndpoint() string {
	return this.Endpoint
}

func (this *kinesis) GetStream(table string) string {
	if strings.Contains(this.StreamName, REPLACE) {
		return strings.Replace(this.StreamName, REPLACE, table, -1)
	} else {
		return this.StreamName
	}
}
