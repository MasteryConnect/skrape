package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/apex/go-apex"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
)

type message struct {
	Decode bool `json:"decode"`
}

type response struct {
	Database string `json:"db"`
	Schema   string `json:"schema"`
}

type Credentials struct {
	User     string `json:"user"`
	Pwd      string `json:"pwd"`
	Port     int    `json:"port"`
	Host     string `json:"host"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
}

type AppErr struct {
	Message string `json:"msg"`
	Error   string `json:"msg"`
}

func main() {

	log.SetHandler(text.New(os.Stderr))

	var svc = kms.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))}))
	apex.HandleFunc(func(event json.RawMessage, ctx *apex.Context) (interface{}, error) {
		file, err := os.Open("creds.encrypted")
		if err != nil {
			return nil, err
		}
		read, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}

		encrypted, err := svc.Decrypt(&kms.DecryptInput{CiphertextBlob: read})
		if err != nil {
			return nil, err
		}

		creds := &Credentials{}
		json.Unmarshal(encrypted.Plaintext, creds)
		if err != nil {
			return nil, err
		}

		//var r response

		//if err := json.Unmarshal(event, &m); err != nil {
		//	return nil, err
		//}

		//return "done", nil
		//})

		return creds, nil
	})
}

func JsonError(msg, err string) []byte {
	apperr := AppErr{msg, err}
	encoded, _ := json.Marshal(apperr)
	return encoded
}
