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
	cpuProfileFile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)

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
		cmd := exec.Command("go", "tool", "pprof", "-web", "/tmp/___go_build_main_go", cpuProfileFile.Name())
		cmd.Run()
	}
}
