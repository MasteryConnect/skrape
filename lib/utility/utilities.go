package utility

import (
	"fmt"
	"os"
	"strings"
)

func Cleanup(path string) {
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

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func MoveToFrontOfSlice(a, b []string) []string {
	for _, c := range a {
		for i, d := range b {
			if c == d {
				b = append(b[:i], b[i+1:]...)
				b = append([]string{c}, b...)
			}
		}
	}
	return b
}

func SlcDelFrmSlc(a, b []string) []string {
	for _, c := range a {
		for i, d := range b {
			if c == d {
				copy(b[i:], b[i+1:])
				b[len(b)-1] = ""
				b = b[:len(b)-1]
			}
		}
	}
	return b
}

func StrDelFrmSlc(a string, b []string) []string {
	for i, c := range b {
		if c == a {
			copy(b[i:], b[i+1:])
			b[len(b)-1] = ""
			b = b[:len(b)-1]
		}
	}
	return b
}

// Convert Mysql INSERT values list into the correct CSV format
func MysqlInsertValuesToCsv(values string) string {
	csv := strings.Replace(values, "\\\"", "\"\"", -1)
	csv = strings.Replace(csv, ",'", ",\"", -1)
	return strings.Replace(csv, "',", "\",", -1)
}

// Check the array for strings with commas. If a string with commas is found
// split it up and append to the array. Example: and array with
// [a,b c d,e] ends up looking like [a b c d e]
func ExtractAndAppendCommaDelimitedStrings(values []string) []string {
	var newValues []string
	for _, val := range values {
		newValues = append(newValues, strings.Split(val, ",")...)
	}
	return newValues
}
