package mysqlutils

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/apex/log"
)

const (
	MacOsX           = "/usr/local/bin/mysqldump"
	Linux            = "/usr/bin/mysqldump"
	MysqlDumpVersion = 10.13
)

var mysqlDumpPath string

func VerifyMysqldump(path string) {
	found := false
	// Check custom path if it exists
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			found = true
		}
	}

	// Check typical OS locations (mac, linux)
	if _, err := os.Stat(MacOsX); err == nil {
		found = true
	} else if _, err = os.Stat(Linux); err == nil {
		found = true
	}

	// Finally ask the OS for the version
	if found == true {
		if questionOs(path) == true {
			found = true
		} else {
			found = false
		}
	}

	if found == false {
		printWarning()
	}
}

func questionOs(path string) bool {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		log.Fatal(err.Error())
	}

	// retrieves full custom path, or just binary name
	mysqlDumpPath = getPath(path)

	cmd := exec.Command(mysqlDumpPath, "--version")
	cmd.Stdout = w

	if err = cmd.Start(); err != nil {
		log.Fatal(err.Error())
	}
	cmd.Wait()
	if err = w.Close(); err != nil {
		log.Fatal(err.Error())
	}
	out, _ := ioutil.ReadAll(r)
	os.Stdout = oldStdout

	rgxp, err := regexp.Compile(".*Ver ([0-9]{1,2}.[0-9]{1,2})")
	if err != nil {
		log.Fatal(err.Error())
	}

	// Match the regex, convert it to an 8bit float and compare
	match := rgxp.FindSubmatch(out)
	version, err := strconv.ParseFloat(string(match[1]), 8)
	if version >= MysqlDumpVersion {
		return true
	}
	return false
}

func printWarning() {
	str := `mysqldump Ver 10.13 or greater is required.
 The mysqldump binary is either not installed
 or is located in a non-typical location. If you are
 certain that it is installed please restart the app
 using the --mysqldump-path flag to point to where the binary
 is located.
`
	fmt.Println(str)
	os.Exit(1)
}

func getPath(path string) (p string) {
	rgxp, _ := regexp.Compile(`((/[a-zA-Z_/]+/?[^mysqldump])+)`)
	matches := rgxp.FindStringSubmatch(path)
	if matches != nil {
		s := matches[0]
		if last := s[len(s)-1]; string(last) == "/" {
			p = fmt.Sprintf("%smysqldump", s)
		} else {
			p = fmt.Sprintf("%s/mysqldump", s)
		}
	} else {
		p = "mysqldump"
	}
	return
}

func GetBinary() string {
	return mysqlDumpPath
}
