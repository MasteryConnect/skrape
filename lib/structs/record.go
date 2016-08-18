package structs

import (
	"encoding/json"
	"fmt"
)

type Record map[string]interface{}

func (this *Record) GetID() string {
	return fmt.Sprintf("%d", (*this)["id"])
}

func (this Record) String() string {
	return fmt.Sprintf("%15s", this["id"])
}

func (this *Record) Json() ([]byte, error) {
	return json.Marshal(this)
}
