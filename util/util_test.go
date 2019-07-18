package util

import (
	"testing"
)

func TestIsSubsetYes(t *testing.T) {
	a := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	b := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	result := IsSubset(a, b)

	if !result {
		t.Error("should return true")
	}
}
func TestIsSubsetMissing(t *testing.T) {
	a := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	b := map[string]string{
		"key1": "value1",
		"key3": "value3",
	}

	result := IsSubset(a, b)

	if result {
		t.Error("should return false")
	}
}

func TestIsSubsetDifferent(t *testing.T) {
	a := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	b := map[string]string{
		"key1": "value1",
		"key2": "value4",
		"key3": "value3",
	}

	result := IsSubset(a, b)

	if result {
		t.Error("should return false")
	}
}
