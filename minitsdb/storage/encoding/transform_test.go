package encoding

import (
	"testing"
)

func testTransform(t *testing.T, tr Transformer) {
	input := []int64{6, 7, 8, 9, 10, 10, 11, 12, 14, 15, 16, 18, 20, 22, 24, 26, 28}

	// make a copy to ensure that the apply method does not modify it's input
	inputCopy := make([]int64, len(input))
	copy(inputCopy, input)

	// apply transformation
	encoded, err := Apply(inputCopy)

	if err != nil {
		t.Error(err)
		return
	}

	// check input argument for modification
	for i := range input {
		if input[i] != inputCopy[i] {
			t.Errorf("Apply function modified input slice \n%v\n%v", input, inputCopy)
			return
		}
	}

	// revert transformation
	decoded, err := Revert(encoded)

	if err != nil {
		t.Error(err)
		return
	}

	// check input and output match
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
