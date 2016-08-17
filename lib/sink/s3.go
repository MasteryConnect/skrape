package sink

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/MasteryConnect/skrape/lib/setup"
	"github.com/MasteryConnect/skrape/lib/skrape/skrapes3"
	"github.com/apex/log"
)

type S3Sink struct {
	*CsvSink
	Connection *setup.Connection
}

type Schema struct {
	Fields []Field `json:"fields"`
}

type Paths struct {
	JsonPaths []string `json:"jsonpaths"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Null string `json:"null"`
}

func NewS3Sink(path, name string, bufferSize int, conn *setup.Connection) *S3Sink {
	cs := &S3Sink{
		Connection: conn,
		CsvSink:    NewCsvSink(path, name, bufferSize),
	}
	return cs
}

func (s *S3Sink) ReadFinished() {
	// Flush and close file
	s.SinkCore.Close()
	s.File.Close()
	// Upload file
	log.WithField("TableName", s.Name).Info("Uploading file")
	skrapes3.Gzipload(s.Name, s.Path)
	s.Schema()
}

func (s *S3Sink) Close() {
}

// Export a table schema to S3
func (s *S3Sink) Schema() {
	db := s.Connection.Connect()
	schema := Schema{[]Field{}}
	paths := Paths{[]string{}}
	schemaname := s.Name + ".json"
	pathsname := s.Name + "paths.json"
	schemafile, _ := os.Create(s.Path + "/" + schemaname)
	pathsfile, _ := os.Create(s.Path + "/" + pathsname)

	defer db.Close()
	defer schemafile.Close()
	defer pathsfile.Close()

	query := fmt.Sprintf("select COLUMN_NAME as `Field`, COLUMN_TYPE as `Type`, IS_NULLABLE AS `Null` from information_schema.COLUMNS WHERE TABLE_NAME = '%s'", s.Name)

	rows, err := db.Query(query)
	if err != nil {
		log.WithField("error", err).Fatal("there was an error extracting the schema for:" + s.Name)
	}
	for rows.Next() {
		var f Field
		rows.Scan(&f.Name, &f.Type, &f.Null)
		paths.JsonPaths = append(paths.JsonPaths, fmt.Sprintf("$['%s']", f.Name))
		schema.Fields = append(schema.Fields, f)
	}

	encoder := json.NewEncoder(schemafile)
	encoder.Encode(schema)
	schemafile.Sync()

	encoder = json.NewEncoder(pathsfile)
	encoder.Encode(paths)
	pathsfile.Sync()

	skrapes3.S3Upload(schemafile, os.Getenv("S3_BUCKET"), fmt.Sprintf("%s/%s/schemas/%s", os.Getenv("S3_KEY"), skrapes3.S3DateKey(), schemaname))
	os.Remove(s.Path + "/" + schemaname)
	log.Info("Schema uploaded for: " + s.Name)

	skrapes3.S3Upload(pathsfile, os.Getenv("S3_BUCKET"), fmt.Sprintf("%s/%s/paths/%s", os.Getenv("S3_KEY"), skrapes3.S3DateKey(), pathsname))
	os.Remove(s.Path + "/" + pathsname)
	log.Info("JSONPaths file uploaded for: " + s.Name)
}
