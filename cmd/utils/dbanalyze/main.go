package main

// todo:
//  - add check flag -> check timestamp order

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/martin2250/minitsdb/util/analyzedb"
)

var opts struct {
	pathInput string
}

func init() {
	flag.StringVar(&opts.pathInput, "input", "", "input file")

	flag.Parse()
}

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func formatFileSize(n int64) string {
	if n < 0 {
		return fmt.Sprintf("%d B", n)
	}

	var nf = float64(n)
	var e = 0

	for nf > 1024 {
		nf /= 1024
		e++
	}

	if e > 5 {
		return fmt.Sprintf("%0.3e B", float64(n))
	}
	if e == 0 {
		return fmt.Sprintf("%d B", n)
	} else {
		return fmt.Sprintf("%0.1f %ciB", nf, "KMGTPE"[e-1])
	}
}

func main() {
	fileInput, err := os.Open(opts.pathInput)
	check(err)
	defer fileInput.Close()

	fmt.Printf("analyzing %s\n", opts.pathInput)

	result, err := analyzedb.Analyze(fileInput)
	check(err)

	fmt.Printf(" number of blocks: %d\n", result.NumBlocks)
	fmt.Printf(" number of points: %d\n", result.NumPoints)
	fmt.Printf("number of columns: %d\n", result.NumColumns)
	fmt.Printf("   bits per value: %0.2f\n",
		float64(8*result.BytesTotal)/float64(result.NumPoints)/float64(result.NumColumns))
	fmt.Printf("points / block:\n")
	fmt.Printf("              avg: %0.2f\n", result.PointsMean)
	fmt.Printf("            stdev: %0.2f\n", result.PointsStdev)
	fmt.Printf("              min: %d\n", result.PointsMin)
	fmt.Printf("              max: %d\n", result.PointsMax)
	fmt.Printf("%s of %s used (%0.2f%%)\n",
		formatFileSize(result.BytesUsed),
		formatFileSize(result.BytesTotal),
		100*float64(result.BytesUsed)/float64(result.BytesTotal))
}
