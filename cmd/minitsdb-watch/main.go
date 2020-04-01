package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/martin2250/minitsdb/pkg/apiclient"
	"net/http"
)
import "log"

var conf Configuration
var Bot *tgbotapi.BotAPI

func report(message string) {
	msg := tgbotapi.NewMessage(conf.Telegram.ChatID, message)
	Bot.Send(msg)
}

func Notify(series map[string]string, column map[string]string, message string) {
	fmt.Println(series, column, message)
	report(fmt.Sprintf("error in series\n%v\ncolumn\n%v\n%s", series, column, message))
}

func main() {
	opts := readCommandLineOptions()

	conf = readConfigurationFile(opts.ConfigPath)

	var err error
	Bot, err = tgbotapi.NewBotAPI(conf.Telegram.ApiToken)
	if err != nil {
		log.Panic(err)
	}

	client := apiclient.ApiClient{
		Address:    conf.Address,
		HttpClient: http.DefaultClient,
	}

	for _, c := range conf.Watches {
		w := Watcher{
			Config: c,
			Client: client,
			Notify: Notify,
		}
		go w.Run()
	}

	select {}
}
