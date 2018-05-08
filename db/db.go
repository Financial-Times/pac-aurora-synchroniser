package db

import (
	"database/sql"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

func New(dbURL string, maxConnections int) (*sql.DB, error) {
	i := strings.Index(dbURL, ":")
	if i == -1 {
		log.Infof("Connecting to %s", dbURL)
	} else {
		j := strings.Index(dbURL, "@")
		log.Infof("Connecting to %s:********@%s", dbURL[:i], dbURL[j+1:])
	}

	db, err := sql.Open("mysql", dbURL)

	if err == nil {
		err = db.Ping() // force a meaningful connection check
		// we may return a *sql.DB even when there seems to be a connection error - it might recover
	}

	log.Infof("Maximum DB connections = %v", maxConnections)
	db.SetMaxOpenConns(maxConnections)

	return db, err
}
