package insert

import (
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"sort"
)

var insertflags = struct {
	series string
	input  string
	format string
	output string
	buffer int
}{}

type inputFormat interface {
	// Register passes the input series' columns to the inputFormat
	Register([]minitsdb.Column)
	// Read reads the entire input file to RAM
	Read(path string, progress func(p float32)) (storage.PointBuffer, error)
}

var formats = map[string]inputFormat{
	//"series": nil,
	"line": nil,
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "insert",
		Short: "Insert data into a series",
		Long: `
This command will insert data from another file format into the
root bucket of a series.`,
		RunE: run,
	}

	cmd.InitDefaultHelpCmd()

	cmd.Flags().StringVarP(&insertflags.series, "series", "s", "", "path to input series directory")
	cmd.Flags().StringVarP(&insertflags.input, "input", "i", "", "path to input file")
	cmd.Flags().StringVarP(&insertflags.format, "format", "f", "", "input file format")
	cmd.Flags().StringVarP(&insertflags.output, "output", "o", "", "path to output series directory")
	cmd.Flags().IntVarP(&insertflags.buffer, "buffer", "b", 3000, "buffer size")

	return cmd
}

func run(cmd *cobra.Command, args []string) error {
	// check input series
	if stat, err := os.Stat(insertflags.series); os.IsNotExist(err) {
		return errors.New("series points to a nonexisting directory")
	} else if !stat.IsDir() {
		return errors.New("series points to a file")
	}

	// check output series
	if stat, err := os.Stat(insertflags.output); os.IsNotExist(err) {
		if err := os.MkdirAll(insertflags.output, 0600); err != nil {
			return err
		}
	} else if !stat.IsDir() {
		return errors.New("output points to a file")
	} else {
		if dir, err := ioutil.ReadDir(insertflags.output); err != nil {
			return err
		} else if len(dir) > 0 {
			return errors.New("output not empty")
		}
	}

	// check input file
	if stat, err := os.Stat(insertflags.input); os.IsNotExist(err) {
		return errors.New("input points to a nonexisting file")
	} else if stat.IsDir() {
		return errors.New("input points to a directory")
	}

	// check format
	format, ok := formats[insertflags.format]
	if !ok {
		return errors.New("unknown input format")
	}

	// open input series
	logrus.Info("opening input series")
	inputSeries, err := minitsdb.OpenSeries(insertflags.series)
	if err != nil {
		return err
	}

	// open output series
	logrus.Info("opening output series")
	outputSeries, err := minitsdb.OpenSeries(insertflags.output)
	if err != nil {
		return err
	}

	// read input file
	format.Register(inputSeries.Columns)
	data, err := format.Read(insertflags.input, func(p float32) {
		fmt.Printf("%0.0f %% \r", p)
	})
	if err != nil {
		return err
	}
	// sort
	sort.Sort(sortPoints(data))

	return nil
}

type sortPoints storage.PointBuffer

func (s sortPoints) Len() int {
	return len(s.Values[0])
}

func (s sortPoints) Less(i int, j int) bool {
	return s.Values[0][i] < s.Values[0][j]
}

func (s sortPoints) Swap(i int, j int) {
	for k := range s.Values {
		s.Values[k][i], s.Values[k][j] = s.Values[k][j], s.Values[k][i]
	}
}

//type sortPoints [][]int64
//
//func (s sortPoints) Len() int {
//	return len(s[0])
//}
//
//func (s sortPoints) Less(i int, j int) bool {
//	return s[0][i] < s[0][j]
//}
//
//func (s sortPoints) Swap(i int, j int) {
//	for k := range s {
//		s[k][i], s[k][j] = s[k][j], s[k][i]
//	}
//}
