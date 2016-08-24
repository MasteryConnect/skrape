package config

import (
	"strings"
)

type kinesis struct {
	Endpoint   string
	StreamName string
	ShardCount int
}

const REPLACE = "{TABLE_NAME}"

func (c *config) GetKinesis() *kinesis {
	return c.Kinesis
}

func NewKinesis(endpoint, name string, shardCount int) *kinesis {
	return &kinesis{
		Endpoint:   endpoint,
		StreamName: name,
		ShardCount: shardCount,
	}
}

func (k *kinesis) GetEndpoint() string {
	return k.Endpoint
}

func (k *kinesis) GetShardCount() int {
	return k.ShardCount
}

func (k *kinesis) GetStream(table string) string {
	if strings.Contains(k.StreamName, REPLACE) {
		return strings.Replace(k.StreamName, REPLACE, table, -1)
	} else {
		return k.StreamName
	}
}
