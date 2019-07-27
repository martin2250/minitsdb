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

func TestRoundUp(t *testing.T) {
	if RoundUp(59, 20) != 60 {
		t.Error("fail 1")
	}
	if RoundUp(60, 20) != 60 {
		t.Error("fail 1")
	}
	if RoundUp(61, 20) != 80 {
		t.Error("fail 1")
	}
}
