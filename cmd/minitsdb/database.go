package main

import (
	"github.com/martin2250/minitsdb/database"
	log "github.com/sirupsen/logrus"
)

func loadDatabase(dbpath string) database.Database {
	log.WithField("path", dbpath).Info("Loading database")

	db, err := database.NewDatabase(dbpath)
	if err != nil {
		log.WithError(err).Fatal("Failed to load database")
	}

	return db
}
