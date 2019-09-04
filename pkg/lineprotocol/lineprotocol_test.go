package lineprotocol_test

import (
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"reflect"
	"testing"
)

func BenchmarkLineProtocol(b *testing.B) {
	for n := 0; n < b.N; n++ {
		lineprotocol.Parse("name:sensor loc:weatherstation|name:temperature pos:outside 24.34|name:pressure pos:outside 1013.13|name: test 12.1231|124323542354")
	}
}

func TestParse(t *testing.T) {
	type args struct {
		line string
	}
	tests := []struct {
		name    string
		args    args
		want    lineprotocol.Point
		wantErr bool
	}{
		{
			name: "normal",
			args: args{line: "name:main|name:a 1.2|3453453"},
			want: lineprotocol.Point{
				Series: []lineprotocol.KVP{{Key: "name", Value: "main"}},
				Values: []lineprotocol.Value{{
					Tags:  []lineprotocol.KVP{{Key: "name", Value: "a"}},
					Value: 1.2,
				}},
				Time: 3453453,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lineprotocol.Parse(tt.args.line)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}
