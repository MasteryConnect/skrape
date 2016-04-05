package utility

import (
	"fmt"
	"os"

	"github.com/MasteryConnect/skrape/lib/export"
)

func Cleanup() {
	path := export.DefaultFile
	fmt.Println("CLEANING UP")

	if _, err := os.Stat(path); err == nil {
		err := os.Remove(path)
		if err != nil {
			panic(err)
		}
	} else {
		panic(err)
	}
}
