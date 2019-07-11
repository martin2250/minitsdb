package series

// Series describes a time series, id'd by a name and tags
type Series struct {
	Values        [][]int64
	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Path          string
}

// Column holds the json structure that describes a column in a series
type Column struct {
	Name     string
	Decimals int
}

// OpenSeries opens series from file
func OpenSeries(seriespath string) (Series, error) {
	series := Series{}
	var err error

	if err != nil {
		return series, err
	}

	return series, nil
}
