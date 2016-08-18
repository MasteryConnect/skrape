package sink

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/MasteryConnect/skrape/lib/config"
	"github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/MasteryConnect/skrape/lib/skrape/skrapes3"
	"github.com/apex/log"
)

type S3Sink struct {
	*CsvSink
	Cfg config.Config
}

func NewS3Sink(path, name string, bufferSize int, cfg config.Config) *S3Sink {
	cs := &S3Sink{
		Cfg:     cfg,
		CsvSink: NewCsvSink(path, name, bufferSize),
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
	s.CsvSink.Close()
}

// Export a table schema to S3
func (s *S3Sink) Schema() {
	schema, paths := mysqlutils.TableSchema(s.Cfg.GetConn(), s.Name)

	schemaname := s.Name + ".json"
	pathsname := s.Name + "paths.json"
	schemafile, _ := os.Create(s.Path + "/" + schemaname)
	pathsfile, _ := os.Create(s.Path + "/" + pathsname)

	defer schemafile.Close()
	defer pathsfile.Close()

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
