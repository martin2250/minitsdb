package main

import (
	"github.com/martin2250/minitsdb/cmd/minitsdb-util/insert"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "minitsdb-util",
	Short: "miniTSDB Database Utility",
}

func init() {
	rootCmd.InitDefaultHelpCmd()

	rootCmd.AddCommand(insert.NewCommand())
}

func main() {
	rootCmd.Execute()
}
