package main

import (
	"github.com/martin2250/minitsdb/minitsdb"
	log "github.com/sirupsen/logrus"
)

func loadDatabase(dbpath string) minitsdb.Database {
	log.WithField("path", dbpath).Info("Loading database")

	db, err := minitsdb.NewDatabase(dbpath)
	if err != nil {
		log.WithError(err).Fatal("Failed to load database")
	}

	return db
}
