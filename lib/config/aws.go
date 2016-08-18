package config

import (
	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
)

func (this *config) GetAws() *aws.Config {
	if this.Aws == nil {
		log.Debug("AWS Config not defined. Use default config.")
		this.Aws = &aws.Config{Region: aws.String("us-east-1")}
	} else {
		log.Debug("AWS Config already loaded.")
	}
	return this.Aws
}
