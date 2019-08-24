package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// gracefulShutdown waits for a SIGINT or SIGTERM signal
// when a signal was received, it sends true to the channel
// after the timeout, it will force a shutdown
func gracefulShutdown(shutdown chan<- bool, timeout time.Duration) {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	log.Warning("Received shutdown signal")
	shutdown <- true
	time.Sleep(timeout)
	log.Fatal("Graceful shutdown timed out")
}

// shutdownOnError calls the function f, which should never return in normal operation
// when it does, shutdownOnError logs the error and sends a shutdown signal
// to the channel
func shutdownOnError(f func() error, shutdown chan<- bool, timeout time.Duration, message string) {
	err := f()

	log.WithError(err).Warning(message)
	shutdown <- true
	time.Sleep(timeout)
	log.Fatal("Graceful shutdown timed out")
}
