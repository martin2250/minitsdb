package main

import (
	"github.com/rossmcdonald/telegram_hook"
	"github.com/sirupsen/logrus"
	"time"
)

func logAddBackends(conf Configuration) {
	if conf.Telegram != nil {
		hook, err := telegram_hook.NewTelegramHook(
			conf.Telegram.AppName,
			conf.Telegram.AuthToken,
			conf.Telegram.ChatID,
			telegram_hook.WithAsync(true),
			telegram_hook.WithTimeout(30*time.Second),
		)
		if err != nil {
			logrus.WithError(err).Fatalf("failed to create telegram log hook")
		}
		logrus.AddHook(hook)
	}
}
