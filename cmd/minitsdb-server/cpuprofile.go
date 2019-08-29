package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

var cpuProfileFile *os.File
var traceFile *os.File

func startCpuProfile(path string) {
	runtime.SetCPUProfileRate(1000)

	var err error
	cpuProfileFile, err = os.Create(path)

	if err != nil {
		log.WithError(err).Fatal("could not open cpu profile file for writing")
	}

	err = pprof.StartCPUProfile(cpuProfileFile)

	if err != nil {
		log.WithError(err).Fatal("could not start cpu profile")
	}

}

func stopCpuProfile(plot bool) {
	pprof.StopCPUProfile()
	cpuProfileFile.Close()

	if plot {
		exe, err := os.Executable()

		if err != nil {
			log.WithError(err).Warning("could not get binary location")
			return
		}

		cmd := exec.Command("go", "tool", "pprof", "-web", exe, cpuProfileFile.Name())
		cmd.Run()
	}
}

func startTrace(path string) {
	var err error
	traceFile, err = os.Create(path)

	if err != nil {
		log.WithError(err).Fatal("could not open trace file for writing")
	}

	err = trace.Start(traceFile)

	if err != nil {
		log.WithError(err).Fatal("could not start trace")
	}
}

func stopTrace() {
	trace.Stop()
	traceFile.Close()
}
