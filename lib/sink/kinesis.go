package sink

import (
	"strconv"
	"strings"
	"sync"

	"github.com/MasteryConnect/skrape/lib/config"
	"github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/MasteryConnect/skrape/lib/structs"
	"github.com/apex/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisSink struct {
	*SinkCore

	Path    string
	schema  *mysqlutils.Schema
	records []*structs.Record
	svc     *kinesis.Kinesis
	stream  string
}

func NewKinesisSink(path, name string, batchSize int, cfg config.Config) *KinesisSink {
	k := cfg.GetKinesis()
	c := cfg.GetAws()
	if k.GetEndpoint() != "" {
		c = c.WithEndpoint(k.GetEndpoint())
	}
	sess := session.New(c)
	svc := kinesis.New(sess)

	stream := k.GetStream(name)
	log.WithField("name", stream).Info("skrape to stream")

	sink := &KinesisSink{
		records:  make([]*structs.Record, 0, batchSize),
		svc:      svc,
		stream:   stream,
		SinkCore: NewSinkCore(name, batchSize),
	}

	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(stream), // Required
	}
	_, err := svc.DescribeStream(params)

	if err != nil {
		// try creating the stream
		err = sink.createStream(stream)
		if err != nil {
			panic(err)
		}
	}

	sink.schema, _ = mysqlutils.TableSchema(cfg.GetConn(), name)

	return sink
}

// Main writing function for each table.
// this function is responsible for writing
// the exported table to kinesis
func (s *KinesisSink) Write(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		log.Debug("Channel closed, table should be fully exported")
	}()

	for msg := range s.DataChan {
		s.addRecord(msg)
		if len(s.records) == s.BufferSize {
			s.putRecords()
		}
	}
}

// Write out the remaining messages to Kinesis
func (s *KinesisSink) ReadFinished() {
	s.putRecords()
}

func (s *KinesisSink) Close() {
	s.SinkCore.Close()
	s.records = nil
	s.schema = nil
}

func (s *KinesisSink) addRecord(msg string) {
	values := strings.Split(msg, ",")
	record := structs.Record{}

	for i, field := range s.schema.Fields {
		var v interface{}
		val := values[i]
		fieldType := field.Type
		if strings.Contains(field.Type, "(") {
			fieldType = fieldType[:strings.Index(fieldType, "(")]
		}
		switch fieldType {
		case "tinyint", "smallint", "mediumint", "int", "bigint":
			v, _ = strconv.ParseInt(val, 10, 64)
		case "float", "double", "decimal":
			v, _ = strconv.ParseFloat(val, 64)
		default:
			if val != "NULL" {
				v = strings.Trim(val, "'")
			}
		}
		record[field.Name] = v
	}
	s.records = append(s.records, &record)
	log.WithField("record", record).Debug("New record")
}

func (this *KinesisSink) putRecords() error {
	var recordsToDump []*structs.Record
	records := this.records

	// chunk up big batches in case we get a failure and need to retry
	for len(records) > 0 {
		if len(records) > 500 {
			recordsToDump = records[:500]
			records = records[500:]
		} else {
			recordsToDump = records[:]
			records = records[:0]
		}

		retryIdx, err := this._dump(recordsToDump)
		if err != nil {
			return err
		}

		for _, idx := range retryIdx {
			// prepend any records to retry
			records = append(recordsToDump[idx:idx+1], records...)
		}
	}

	return nil
}

func (this *KinesisSink) _dump(recordsToDump []*structs.Record) (retryIdx []int, err error) {
	params := &kinesis.PutRecordsInput{
		StreamName: aws.String(this.stream), // Required
	}

	for _, r := range recordsToDump {
		jsn, err := r.Json()
		if err != nil {
			return retryIdx, err
		}
		params.Records = append(params.Records, &kinesis.PutRecordsRequestEntry{
			Data:         jsn,                   // Required
			PartitionKey: aws.String(r.GetID()), // Required
		})
		log.WithField("dump:", (*r).String()).Debug("record")
	}
	resp, err := this.svc.PutRecords(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		return
	}

	// Pretty-print the response data.
	log.WithField("resp", resp).Debug("Kinesis response")

	if *resp.FailedRecordCount > 0 {
		log.WithFields(log.Fields{
			"count": *resp.FailedRecordCount,
		}).Warn("failed records")

		// handle retry of records that were throttled
		// ProvisionedThroughputExceededException or InternalFailure.
		for i, r := range resp.Records {
			if r.ErrorCode != nil { // on either error, retry
				log.WithFields(log.Fields{
					"Error": *r.ErrorCode,
				}).Warn("failed record")

				retryIdx = append(retryIdx, i)
			}
		}
	}

	return
}

func (this *KinesisSink) createStream(streamName string) error {
	log.WithField("name", streamName).Info("create stream")
	params := &kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),           // Required
		StreamName: aws.String(streamName), // Required
	}

	_, err := this.svc.CreateStream(params)
	if err != nil {
		return err
	}
	return nil
}
