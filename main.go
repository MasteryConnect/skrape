package main

import (
	"os"
	"time"

	"github.com/MasteryConnect/skrape/lib/mysqlutils"
	"github.com/MasteryConnect/skrape/lib/setup"
	"github.com/MasteryConnect/skrape/lib/skrape"
	"github.com/MasteryConnect/skrape/lib/utility"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/codegangsta/cli"
)

const Concurrency = 10

func init() {
}

func main() {
	file, _ := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0660)
	defer file.Close()
	//l, _ := log.ParseLevel("InfoLevel")
	//log.SetLevel(l)
	log.SetHandler(text.New(os.Stdout))

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
		matchTables   bool
		priority      cli.StringSlice
		exclude       cli.StringSlice
	)

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
			Usage:       "number of concurrent goroutines to be spawned (use with caution, this consumes resources)",
			Value:       10,
			Destination: &pool,
		},
		cli.StringSliceFlag{
			Name:  "priority",
			Usage: "declare larger tables as priority (will start these tables exporting first)",
			Value: &priority,
		},
		cli.StringSliceFlag{
			Name:  "exclude",
			Usage: "exclude tables from the export",
			Value: &exclude,
		},
		cli.BoolFlag{
			Name:        "match-table-count M",
			Usage:       "set concurrency level to the number of tables being exported",
			Destination: &matchTables,
		},
	}

	app.Action = func(c *cli.Context) error {
		start := time.Now()
		defer utility.Cleanup(setup.DefaultFile)
		defer func(start time.Time) { // Displays duration of run time
			log.WithFields(log.Fields{
				"Duration": time.Since(start).String(),
			}).Info("Export Completed")
		}(start)

		mysqlutils.VerifyMysqldump(mysqlDumpPath)                                           // make sure that mysqldump is installed
		connect := setup.NewConnection(host, user, port, database, dest, pool, matchTables) // new connection struct
		extract := skrape.NewExtract(connect)
		if !connect.Missing() {
			log.Error("Missing credentials for database connection")
			os.Exit(1)
		}
		setup.MysqlDefaults() // set up defaults file in /tmp to store DB credentials

		if table != "" {
			log.Infof("Performing single table extract for: %s", table)
			chn := make(chan bool)

			go extract.Perform(chn, table)
			chn <- true

		} else {
			extract.TableHandler(priority, exclude)
		}

		return nil
	}
	app.Run(os.Args)
}
