package skrapes3

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var AWSSession *session.Session = session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

type File struct {
	Name string
	Path string
}

func NewFile(name, path string) File {
	return File{name, path}
}

// Stream a gzipped export file to S3
func Gzipload(name, path string) {
	file, err := os.Open(fmt.Sprintf("%s/%s", path, name+".csv"))
	if err != nil {
		log.WithField("error", err).Fatal("There was an error opening the extracted file for gzipping")
	}
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		_, err := io.Copy(gw, file)
		if err != nil {
			log.WithField("error", err).Fatal("There was an error gzipping a file")
		}
		file.Close()
		gw.Close()
		writer.Close()
	}()
	uploader := s3manager.NewUploader(AWSSession)
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(os.Getenv("S3_BUCKET")),
		Key:    aws.String(fmt.Sprintf("%s/%s/data/%s", os.Getenv("S3_KEY"), S3DateKey(), name+".csv.gz")),
	})
	if err != nil {
		log.WithField("error", err).Fatal("Failed to upload file.")
	}
	err = os.Remove(path + "/" + name + ".csv")
	if err != nil {
		log.WithField("error", err).Warn("A csv file was not removed!")
	}
	log.WithField("location", result.Location).Info("Successfully uploaded to")
}

// Upload a file to S3, function will exit
// app with Fatal if there is an issue uploading the file
func S3Upload(body *os.File, bucket, key string) {
	s3Svc := s3.New(AWSSession)

	_, err := s3Svc.PutObject(&s3.PutObjectInput{
		Body:   body,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.WithField("error", err).Fatal(fmt.Sprintf("There was an error uploading %s to S3", body.Name()))
	}
}

// Returns a string consisting of todays date
// to be used as the key (path) for the S3 export
func S3DateKey() (key string) {
	return time.Now().Format("2006/01/02")
}
