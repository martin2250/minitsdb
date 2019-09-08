package main

import (
	"github.com/rossmcdonald/telegram_hook"
	"github.com/sirupsen/logrus"
	"time"
)

func logAddBackends(conf Configuration) {
	if conf.Logging.Telegram != nil {
		hook, err := telegram_hook.NewTelegramHook(
			conf.Logging.Telegram.AppName,
			conf.Logging.Telegram.AuthToken,
			conf.Logging.Telegram.ChatID,
			telegram_hook.WithAsync(true),
			telegram_hook.WithTimeout(30*time.Second),
		)
		if err != nil {
			logrus.WithError(err).Fatalf("failed to create telegram log hook")
		}
		logrus.AddHook(hook)
	}
}
