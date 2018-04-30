package main

import (
	"os"

	"github.com/Financial-Times/pac-aurora-synchroniser/db"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

const (
	appDescription = "A CLI tool to synchronise the latest PAC data between two Aurora instances"
	// db.t2.small allows a maximum of 45 connections, but there are likely 2 instances of this service in each of 2 regions
	maxConnections = 10
)

func main() {
	app := cli.App("pac-aurora-synchroniser", appDescription)

	sourceDBURL := app.String(cli.StringOpt{
		Name:   "source-db-connection-url",
		Desc:   "Connection URL to the database with the most recent data",
		EnvVar: "SOURCE_DB_CONNECTION_URL",
	})

	targetDBURL := app.String(cli.StringOpt{
		Name:   "target-db-connection-url",
		Desc:   "Connection URL to the database that needs to receive the latest data",
		EnvVar: "TARGET_DB_CONNECTION_URL",
	})

	app.Action = func() {

		sourceDB, err := db.New(*sourceDBURL, maxConnections)
		if err != nil {
			log.WithError(err).Fatal("error in connecting to source database")
		}

		targetDB, err := db.New(*targetDBURL, maxConnections)
		if err != nil {
			log.WithError(err).Fatal("error in connecting to target database")
		}

		err = db.Sync(sourceDB, targetDB)
		if err != nil {
			log.WithError(err).Fatal("error in synchronising the two databases")
		}
	}

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Fatal("App could not start")
	}
}
