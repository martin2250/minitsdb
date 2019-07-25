package ingest

import (
	"testing"
)

func TestParsePoint(t *testing.T) {
	p, err := ParsePoint("k1=v1,k2=v2 [name=power,phase=A,a=b]=22.44;[name=power,phase=B]=2 100")

	t.Logf("%+v\n", p)

	if err != nil {
		t.Errorf("error: %v", err)
	}
}
