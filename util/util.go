package util

import "os"

// FileExists checks if file exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// RoundDown rounds down value to next multiple of modulo
// only works for positive integers
func RoundDown(value, modulo int64) int64 {
	if value < 0 {
		return 0
	}
	return (value / modulo) * modulo
}
