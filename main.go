package main

import (
	"os"

	"github.com/MasteryConnect/skrape/lib/export"
	"github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/codegangsta/cli"
)

var host string
var port string
var user string

func init() {
	log.SetHandler(text.New(os.Stderr))
}

func main() {
	// cli flag vars
	var mysqlPath string

	app := cli.NewApp()
	app.Name = "skrape"
	app.Usage = "export MySQL RDBMS tables to csv files"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "username, u",
			Usage:       "username for mysql database",
			Value:       "root",
			Destination: &user,
		},
		cli.StringFlag{
			Name:        "port, P",
			Usage:       "host name for mysql database",
			Value:       "3306",
			Destination: &port,
		},
		cli.StringFlag{
			Name:        "hostname, H",
			Usage:       "host name for mysql database",
			Value:       "127.0.0.1",
			Destination: &host,
		},
		cli.StringFlag{
			Name:        "mysqldump-path",
			Usage:       "set the path for the location of the mysqldump binary (defaults to typical locations for mac/linux)",
			Value:       "",
			Destination: &mysqlPath,
		},
	}
	app.Action = func(c *cli.Context) {
		mysqlutils.VerifyMysqldump(mysqlPath)
		export.Perform(host, user, port)
	}
	app.Run(os.Args)
}
