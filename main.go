package main

import (
	"os"
	"time"

	utility "github.com/MasteryConnect/skrape/lib"
	"github.com/MasteryConnect/skrape/lib/export"
	"github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/codegangsta/cli"
)

const Concurrency = 10

func init() {
	log.SetHandler(text.New(os.Stderr))
}

func main() {
	start := time.Now()
	defer utility.Cleanup()
	defer func(start time.Time) {
		log.WithFields(log.Fields{
			"Duration": time.Since(start).String(),
		}).Info("Completed")
	}(start)

	// cli flag vars
	var (
		mysqlDumpPath string
		host          string
		port          string
		user          string
		database      string
		table         string
		dest          string
		pool          int
	)
	// App vars
	var cncy int

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
		cli.StringFlag{
			Name:        "export-path, e",
			Usage:       "set the path where you wish to export the CSV files",
			Value:       "./",
			Destination: &dest,
		},
		cli.IntFlag{
			Name:        "concurrency, C",
			Usage:       "set the path where you wish to export the CSV files",
			Destination: &pool,
		},
	}

	if pool == 0 {
		cncy = Concurrency
	} else {
		cncy = pool
	}

	app.Action = func(c *cli.Context) {
		mysqlutils.VerifyMysqldump(mysqlDumpPath)
		connect := export.NewConnection(host, user, port, database, dest, cncy)
		export.MysqlDefaults()
		if table != "" {
			params := export.NewParameters(connect, export.NewTable(connect.Destination, table))
			log.Info("Before channel")
			chn := make(chan bool)
			go params.Perform(chn)
			chn <- true
			log.Info("After channel")
		} else {
			connect.TableLookUp()
		}
	}
	app.Run(os.Args)
}
