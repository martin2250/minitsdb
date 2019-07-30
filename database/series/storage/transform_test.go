package storage

import (
	"testing"
)

func testTransform(t *testing.T, transformer Transformer) {
	input := []int64{6, 7, 8, 9, 10, 10, 11, 12, 14, 15, 16, 18, 20, 22, 24, 26, 28}

	encoded, err := transformer.Apply(input)

	if err != nil {
		t.Error(err)
		return
	}

	decoded, err := transformer.Revert(encoded)

	if err != nil {
		t.Error(err)
		return
	}

	for i := range input {
		if input[i] != decoded[i] {
			t.Errorf("decoded array does not match original\n%v\n%v", input, decoded)
			return
		}
	}
}

func TestDiffTransform(t *testing.T) {
	testTransform(t, DiffTransformer{N: -2})
	testTransform(t, DiffTransformer{N: 0})
	testTransform(t, DiffTransformer{N: 1})
	testTransform(t, DiffTransformer{N: 2})
	testTransform(t, DiffTransformer{N: 50})
}
