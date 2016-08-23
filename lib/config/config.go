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

func NewConfig(c *setup.Connection, kinesisStreamEndpoint, kinesisStreamName string, kinesisShardCount int) Config {
	return &config{
		Connection: c,
		Kinesis:    NewKinesis(kinesisStreamEndpoint, kinesisStreamName, kinesisShardCount),
	}
}

func (this *config) GetConn() *setup.Connection {
	return this.Connection
}
