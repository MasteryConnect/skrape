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
	skrapePwd     bool
	priority      cli.StringSlice
	exclude       cli.StringSlice
)

func init() {
}

func main() {
	log.SetHandler(text.New(os.Stdout))

	app := cli.NewApp()
	app.Name = "skrape"
	app.Usage = "export MySQL RDBMS tables"
	app.Version = "1.0"
	// Global flags used by every command
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "u, username",
			Usage:       "username for mysql server",
			Value:       "root",
			Destination: &user,
		},
		cli.StringFlag{
			Name:        "P, port",
			Usage:       "host name for mysql server",
			Value:       "3306",
			Destination: &port,
		},
		cli.StringFlag{
			Name:        "H, hostname",
			Usage:       "host name for mysql server",
			Value:       "127.0.0.1",
			Destination: &host,
		},
		cli.StringFlag{
			Name:        "D, database",
			Usage:       "targeted database",
			Value:       "",
			Destination: &database,
		},
		cli.StringFlag{
			Name:        "t, table",
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
			Name:        "e, export-path",
			Usage:       "set the path where you wish to export the CSV files",
			Value:       "./",
			Destination: &dest,
		},
		cli.IntFlag{
			Name:        "C, concurrency",
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
			Name:        "M, match-table-count",
			Usage:       "set concurrency level to the number of tables being exported",
			Destination: &matchTables,
		},
		cli.BoolFlag{
			Name:        "p, skip-pass",
			Usage:       "do not prompt for password, instead use the env var",
			Destination: &skrapePwd,
			EnvVar:      "SKRAPE_PWD",
		},
	}
	// Commands available. Defaults to s3 if no command given
	app.Commands = []cli.Command{
		{
			Name:    "s3",
			Aliases: []string{"s"},
			Usage:   "export to csv files that are uploaded to s3",
			Action: func(c *cli.Context) error {
				return action(c, "s3")
			},
		},
		{
			Name:    "csv",
			Aliases: []string{"c"},
			Usage:   "export to csv files",
			Action: func(c *cli.Context) error {
				return action(c, "csv")
			},
		},
		{
			Name:    "kinesis",
			Aliases: []string{"k"},
			Usage:   "export to an AWS Kinesis stream",
			Action: func(c *cli.Context) error {
				return action(c, "kinesis")
			},
		},
	}
	// Default action if no command specified
	app.Action = func(c *cli.Context) error {
		return action(c, "s3")
	}
	app.Run(os.Args)
}

func action(c *cli.Context, sinkType string) error {
	start := time.Now()
	defer utility.Cleanup(setup.DefaultFile)
	defer func(start time.Time) { // Displays duration of run time
		log.WithFields(log.Fields{
			"Duration": time.Since(start).String(),
		}).Info("Export Completed")
	}(start)

	mysqlutils.VerifyMysqldump(mysqlDumpPath)                                                      // make sure that mysqldump is installed
	connect := setup.NewConnection(host, user, port, database, dest, pool, matchTables, skrapePwd) // new connection struct
	extract := skrape.NewExtract(sinkType, connect)
	if !connect.Missing() {
		log.Error("Missing credentials for database connection")
		os.Exit(1)
	}
	setup.MysqlDefaults(skrapePwd) // set up defaults file in /tmp to store DB credentials

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
