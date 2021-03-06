package config

import (
	"github.com/MasteryConnect/skrape/lib/setup"
	"github.com/aws/aws-sdk-go/aws"
)

type Config interface {
	GetKinesis() *kinesis
	GetAws() *aws.Config
	GetConn() *setup.Connection
}

type config struct {
	Aws        *aws.Config
	Kinesis    *kinesis
	Connection *setup.Connection
}

func NewConfig(c *setup.Connection, region, kinesisStreamEndpoint, kinesisStreamName string, kinesisShardCount int) Config {
	return &config{
		Connection: c,
		Aws:        NewAws(region),
		Kinesis:    NewKinesis(kinesisStreamEndpoint, kinesisStreamName, kinesisShardCount),
	}
}

func (c *config) GetConn() *setup.Connection {
	return c.Connection
}
