package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"runtime/pprof"
)

var cpuProfileFile *os.File

func startCpuProfile(path string) {
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
