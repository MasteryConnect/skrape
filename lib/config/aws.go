package config

import (
	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
)

func NewAws(region string) *aws.Config {
	return &aws.Config{Region: aws.String(region)}
}

func (this *config) GetAws() *aws.Config {
	if this.Aws == nil {
		log.Debug("AWS Config not defined. Use default config.")
		this.Aws = NewAws("us-east-1")
	} else {
		log.Debug("AWS Config already loaded.")
	}
	return this.Aws
}
