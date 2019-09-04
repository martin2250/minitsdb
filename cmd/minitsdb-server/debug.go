package main

import (
	"github.com/pkg/profile"
	"github.com/sirupsen/logrus"
)

func debugStartProfile(prof, profPath string) interface{ Stop() } {
	var p func(p *profile.Profile)
	switch prof {
	case "cpu":
		p = profile.CPUProfile
	case "mem":
		p = profile.MemProfile
	case "mutex":
		p = profile.MutexProfile
	case "block":
		p = profile.BlockProfile
	case "thread":
		p = profile.ThreadcreationProfile
	case "trace":
		p = profile.TraceProfile
	default:
		logrus.WithField("profile", prof).Fatal("Unknown profile type")
	}

	popts := []func(p *profile.Profile){p, profile.NoShutdownHook}

	if profPath != "" {
		popts = append(popts, profile.ProfilePath(profPath))
		popts = append(popts, profile.Quiet)
	}

	return profile.Start(popts...)
}
