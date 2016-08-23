package sink

import (
	"encoding/csv"
	"strconv"
	"strings"
	"sync"
	"time"

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

	Path            string
	schema          *mysqlutils.Schema
	records         []*structs.Record
	svc             *kinesis.Kinesis
	stream          string
	kinesisPutCount int64
	kinesisErrCount int64
	tickerDoneChan  chan bool
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
		records:        make([]*structs.Record, 0, batchSize),
		svc:            svc,
		stream:         stream,
		tickerDoneChan: make(chan bool),
		SinkCore:       NewSinkCore(name, batchSize),
	}

	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(stream), // Required
	}
	_, err := svc.DescribeStream(params)

	if err != nil {
		// try creating the stream
		err = sink.createStream(stream, k.GetShardCount())
		if err != nil {
			panic(err)
		}
	}

	sink.schema, _ = mysqlutils.TableSchema(cfg.GetConn(), name)

	tickChan := time.NewTicker(time.Second * 10).C
	go func() {
		for {
			select {
			case <-tickChan:
				log.WithFields(log.Fields{
					"records":   len(sink.records),
					"put count": sink.kinesisPutCount,
					"put err":   sink.kinesisErrCount,
					"dataChan":  len(sink.DataChan),
				}).Info("Stats")
			case <-sink.tickerDoneChan:
				return
			}
		}
	}()

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
		err := s.addRecord(msg)
		if err != nil {
			log.WithFields(log.Fields{"err": err, "msg": msg}).Warn("Add record error")
			return
		}

		if len(s.records) >= s.BufferSize {
			err := s.putRecords()
			if err != nil {
				log.WithField("err", err).Warn("Put record error")
				return
			}
		}
	}
}

// Write out the remaining messages to Kinesis
func (s *KinesisSink) ReadFinished() {
	s.putRecords()
	s.tickerDoneChan <- true
	log.WithField("count", len(s.records)).Info("Record count")
}

func (s *KinesisSink) Close() {
	s.SinkCore.Close()
	s.records = nil
	s.schema = nil
}

func (s *KinesisSink) addRecord(msg string) error {
	values, err := s.split(msg, s.schema.ColCount)
	if err != nil {
		return err
	}
	record := structs.Record{}
	record["deltatype"] = "1" // Create record

	// if len(values) != s.schema.ColCount {
	// 	log.WithFields(log.Fields{
	// 		"values length": len(values),
	// 		"column count":  s.schema.ColCount,
	// 		"records":       len(s.records),
	// 		"put count":     s.kinesisPutCount,
	// 		"put err":       s.kinesisErrCount,
	// 		"dataChan":      len(s.DataChan),
	// 	}).Info("Column vs. values length mismatch")
	// }

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
				v = val
			}
		}
		record[field.Name] = v
	}
	s.records = append(s.records, &record)
	log.WithField("record", record).Debug("New record")
	return nil
}

func (this *KinesisSink) putRecords() error {
	var recordsToDump []*structs.Record

	// chunk up big batches in case we get a failure and need to retry
	for len(this.records) > 0 {
		if len(this.records) > 500 {
			recordsToDump = this.records[:500]
			this.records = this.records[500:]
		} else {
			recordsToDump = this.records[:]
			this.records = this.records[:0]
		}

		retryIdx, err := this._dump(recordsToDump)
		if err != nil {
			return err
		}

		for _, idx := range retryIdx {
			// prepend any records to retry
			this.records = append(recordsToDump[idx:idx+1], this.records...)
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

	putCount := int64(len(recordsToDump))
	if *resp.FailedRecordCount > 0 {
		this.kinesisErrCount += *resp.FailedRecordCount
		putCount -= *resp.FailedRecordCount
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
	this.kinesisPutCount += putCount

	return
}

func (this *KinesisSink) createStream(streamName string, shardCount int) error {
	log.WithField("name", streamName).Info("create stream")
	params := &kinesis.CreateStreamInput{
		ShardCount: aws.Int64(int64(shardCount)), // Required
		StreamName: aws.String(streamName),       // Required
	}

	_, err := this.svc.CreateStream(params)
	if err != nil {
		return err
	}
	return nil
}

func (this *KinesisSink) split(msg string, count int) ([]string, error) {
	newMsg := strings.Replace(msg, "\\\"", "\"\"", -1)
	newMsg = strings.Replace(newMsg, ",'", ",\"", -1)
	newMsg = strings.Replace(newMsg, "',", "\",", -1)
	r := csv.NewReader(strings.NewReader(newMsg))

	return r.Read()
}
