// +build gofuzz

package lineprotocol

func Fuzz(data []byte) int {
	if _, err := Parse(string(data)); err != nil {
		return 0
	}
	return 1
}
