package series

import (
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
)

// Bucket is a downsampling step
// Bucket only describes data stored permanently in files, not
type Bucket struct {
	Series Series
	// TimeLast      int64 // timestamp of last value stored in file (indicates when to downsample data)
	TimeStep      int64 // time between points
	PointsPerFile int64
}

// GetPath returns the path where database files are stored
func (b Bucket) GetPath() string {
	return path.Join(b.Series.Path, strconv.FormatInt(b.TimeStep, 10))
}

// GetFileName returns the name of the database file that starts at time
func (b Bucket) GetFileName(time int64) string {
	return path.Join(b.GetPath(), fmt.Sprintf("%011d.mdb", time))
}

// GetDataFiles returns a list of the starting times of all database files
func (b Bucket) GetDataFiles() ([]int64, error) {
	files, err := ioutil.ReadDir(b.GetPath())

	if err != nil {
		return nil, err
	}

	list := make([]int64, 0)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var fileStartTime int64
		if n, err := fmt.Sscanf(file.Name(), "%d.mdb", &fileStartTime); err != nil || n != 1 {
			continue
		}

		list = append(list, fileStartTime)
	}

	return list, nil
}
