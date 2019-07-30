package analyzedb

import (
	"fmt"
	"io"
	"math"

	"github.com/martin2250/minitsdb/database/series/storage"
)

// ErrColumnMismatch indicates that blocks in a file have inconsistent numbers of columns
var ErrColumnMismatch = fmt.Errorf("Number of columns inconsistent within file")

// ErrFileEmpty indicates file empty
var ErrFileEmpty = fmt.Errorf("File is empty")

// AnalyzeResult groups result of analysis
type AnalyzeResult struct {
	NumBlocks, NumColumns           int
	NumPoints, PointsMin, PointsMax int64
	PointsMean, PointsStdev         float64
	BytesUsed, BytesTotal           int64
}

// Analyze reads all blocks from a file, decodes the headers and calculates statistics
func Analyze(r io.ReadSeeker) (result AnalyzeResult, err error) {
	header, err := storage.DecodeHeader(r)

	if err != nil {
		if err == io.EOF {
			err = ErrFileEmpty
		}
		return
	}

	result.NumColumns = int(header.NumColumns)
	result.NumBlocks = 1

	result.NumPoints = int64(header.NumPoints)
	result.PointsMin = int64(header.NumPoints)
	result.PointsMax = int64(header.NumPoints)

	result.BytesUsed = int64(header.BytesUsed)
	result.BytesTotal = 4096

	var pointsSquared = result.NumPoints * result.NumPoints

	for {
		_, err = r.Seek(result.BytesTotal, io.SeekStart)

		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}

		header, err = storage.DecodeHeader(r)

		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}

		if int(header.NumColumns) != result.NumColumns {
			err = ErrColumnMismatch
			return
		}

		numPoints := int64(header.NumPoints)

		if numPoints < result.PointsMin {
			result.PointsMin = numPoints
		}
		if numPoints > result.PointsMax {
			result.PointsMax = numPoints
		}

		result.NumPoints += numPoints
		pointsSquared += numPoints * numPoints
		result.NumBlocks++

		result.BytesUsed += int64(header.BytesUsed)
		result.BytesTotal += 4096
	}

	result.PointsMean = float64(result.NumPoints) / float64(result.NumBlocks)
	result.PointsStdev = math.Sqrt(float64(pointsSquared)/float64(result.NumBlocks) - result.PointsMean*result.PointsMean)

	return
}
