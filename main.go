package main

import (
	"os"

	utility "github.com/MasteryConnect/skrape/lib"
	"github.com/MasteryConnect/skrape/lib/export"
	"github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/codegangsta/cli"
)

func init() {
	log.SetHandler(text.New(os.Stderr))
}

func main() {
	defer utility.Cleanup()
	// cli flag vars
	var mysqlDumpPath string
	var host string
	var port string
	var user string
	var database string
	var table string
	ch := make(chan bool)

	app := cli.NewApp()
	app.Name = "skrape"
	app.Usage = "export MySQL RDBMS tables to csv files"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "username, u",
			Usage:       "username for mysql server",
			Value:       "root",
			Destination: &user,
		},
		cli.StringFlag{
			Name:        "port, P",
			Usage:       "host name for mysql server",
			Value:       "3306",
			Destination: &port,
		},
		cli.StringFlag{
			Name:        "hostname, H",
			Usage:       "host name for mysql server",
			Value:       "127.0.0.1",
			Destination: &host,
		},
		cli.StringFlag{
			Name:        "database, D",
			Usage:       "targeted database",
			Value:       "",
			Destination: &database,
		},
		cli.StringFlag{
			Name:        "table, t",
			Usage:       "name of the table to be exported",
			Value:       "",
			Destination: &table,
		},
		cli.StringFlag{
			Name:        "mysqldump-path",
			Usage:       "set the path for the location of the mysqldump binary (defaults to typical locations for mac/linux)",
			Value:       "",
			Destination: &mysqlDumpPath,
		},
	}

	app.Action = func(c *cli.Context) {
		mysqlutils.VerifyMysqldump(mysqlDumpPath)
		params := export.NewParameters(host, user, port, database)
		params.MysqlDefaults()
		if table != "" {
			params.Table = table
			params.All = false
			params.Perform(ch)
			x := <-ch
			if x == true {
				log.Info("completed successfully")
			}
		} else {
			params.TableLookUp()
		}
	}
	app.Run(os.Args)
}
