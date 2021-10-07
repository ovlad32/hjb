package serde

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

type SS struct{}
type XWriteToTest struct {
	inputI  []int
	inputS  []string
	inputSS [][]*SS
	name    string
}

func (s *SS) ReadFrom(r io.Reader) (n int64, err error) {
	panic("here")
	return 0, nil
}

func (s *SS) WriteTo(w io.Writer) (int64, error) {
	panic("here2")
	return 0, nil
}

var XWriteToTests = []XWriteToTest{
	{
		inputI: []int{3},
		name:   "1 element",
	},
	{
		inputI: []int{3, 5},
		name:   "2 elements",
	},
	{
		inputI: []int{3, 5, 1000001},
		name:   "3 elements",
	},
	{
		inputI: []int{3},
		inputS: []string{"a"},
		name:   "1+1 elements",
	},
	{
		inputI: []int{3, 2},
		inputS: []string{"a", "b"},
		name:   "2+2 elements",
	},
	{
		inputI: []int{3, 0, 2, 0},
		inputS: []string{"", "b", "", "c"},
		name:   "4+4 elements",
	},
	{
		inputI: []int{3, 0, 2, 0},
		inputS: []string{"", "b", "", "c"},
		inputSS: [][]*SS{
			[]*SS{{}, nil, {}, nil, {}},
			[]*SS{nil, {}, {}, nil, {}},
			[]*SS{nil, {}, nil, {}, nil},
		},
		name: "4+4+5 elements",
	},
}

func Test_xWriteTo(t *testing.T) {
	var b = new(bytes.Buffer)
	for _, test := range XWriteToTests {
		b.Reset()
		for i := 0; i < len(test.inputI) || i < len(test.inputS) || i < len(test.inputSS); i++ {
			if test.inputI != nil && i < len(test.inputI) {
				if test.inputI[i] != 0 {
					IntWriteTo(b, int64(test.inputI[i]))
				}
			}
			if test.inputS != nil && i < len(test.inputS) {
				if test.inputS[i] != "" {
					StringWriteTo(b, test.inputS[i])
				}
			}
			if test.inputSS != nil && i < len(test.inputSS) {
				if test.inputSS[i] != nil {
					x := test.inputSS[i]
					_, err := SliceWriteTo(b, x)
					if err != nil {
						t.Fatal(err)

					}
				}
			}
		}

		for i := 0; i < len(test.inputI) || i < len(test.inputS) || i < len(test.inputSS); i++ {
			if test.inputI != nil && i < len(test.inputI) {
				if test.inputI[i] != 0 {
					var r int64
					IntReadFrom(&r, b)
					if int(r) != test.inputI[i] {
						t.Fatalf("Test %v: expected %v, got %v", test.name, test.inputI[i], r)
					}
				}
			}
			if test.inputS != nil && i < len(test.inputS) {
				if test.inputS[i] != "" {
					var r string
					StringReadFrom(&r, b)
					if r != test.inputS[i] {
						t.Fatalf("Test %v: expected %v, got %v", test.name, test.inputS[i], r)
					}
				}
			}
			if test.inputSS != nil && i < len(test.inputSS) {
				if test.inputSS[i] != nil {
					var r []string
					_, err := SliceReadFrom(func(n int) interface{} {
						return make([]string, 0, n)
					}, b)
					if err != nil {
						t.Fatal(err)

					}
					if !reflect.DeepEqual(r, test.inputSS[i]) {
						t.Fatalf("Test %v: expected %v, got %v", test.name, test.inputSS[i], r)
					}
				}
			}
		}

	}
}
