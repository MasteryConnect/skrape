package mysqlutils

import (
	"fmt"

	"github.com/MasteryConnect/skrape/lib/setup"
	"github.com/apex/log"
)

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

// Get the table schema
func TableSchema(conn *setup.Connection, tableName string) (*Schema, *Paths) {
	db := conn.Connect()
	schema := Schema{[]Field{}}
	paths := Paths{[]string{}}

	defer db.Close()

	query := fmt.Sprintf("select COLUMN_NAME as `Field`, COLUMN_TYPE as `Type`, IS_NULLABLE AS `Null` from information_schema.COLUMNS WHERE TABLE_NAME = '%s'", tableName)

	rows, err := db.Query(query)
	if err != nil {
		log.WithField("error", err).Fatal("there was an error extracting the schema for:" + tableName)
	}
	for rows.Next() {
		var f Field
		rows.Scan(&f.Name, &f.Type, &f.Null)
		paths.JsonPaths = append(paths.JsonPaths, fmt.Sprintf("$['%s']", f.Name))
		schema.Fields = append(schema.Fields, f)
	}

	return &schema, &paths
}
