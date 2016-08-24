package structs

import (
	"encoding/json"
	"fmt"
)

type Record map[string]interface{}

func (rec *Record) GetID() string {
	return fmt.Sprintf("%d", (*rec)["id"])
}

func (rec Record) String() string {
	return fmt.Sprintf("%15s", rec["id"])
}

func (rec *Record) Json() ([]byte, error) {
	return json.Marshal(rec)
}
