package config

import (
	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
)

func NewAws(region string) *aws.Config {
	return &aws.Config{Region: aws.String(region)}
}

func (c *config) GetAws() *aws.Config {
	if c.Aws == nil {
		log.Debug("AWS Config not defined. Use default config.")
		c.Aws = NewAws("us-east-1")
	} else {
		log.Debug("AWS Config already loaded.")
	}
	return c.Aws
}
