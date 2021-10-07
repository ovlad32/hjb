package iix
/*
import (
	"testing"
)

func Test_seqInclusion(t *testing.T) {
	type tCase struct {
		name   string
		expect setType
		s1     string
		s2     string
		onChar byte
	}
	tCases := []tCase{
		{name: "values are empty",
			expect: emptySet, s1: "", s2: "", onChar: '1'},
		{name: "values are same and length of 1",
			expect: fullSet, s1: "1", s2: "1", onChar: '1'},
		{name: "values are NOT same and length of 1",
			expect: emptySet, s1: "0", s2: "1", onChar: '1'},
		{name: "values are same - 1",
			expect: fullSet, s1: "1111", s2: "1111", onChar: '1'},
		{name: "values are same - 2",
			expect: fullSet, s1: "0101", s2: "0101", onChar: '1'},
		{name: "values are same - 3",
			expect: fullSet, s1: "101", s2: "101", onChar: '1'},
		{name: "values are same - 4",
			expect: fullSet, s1: "xxxx", s2: "xxxx", onChar: '1'},
		{name: "same length - not equal - 1",
			expect: emptySet, s1: "1001", s2: "0101", onChar: '1'},
		{name: "same length - not equal - 2",
			expect: emptySet, s1: "1011", s2: "0101", onChar: '1'},
		{name: "same length - superset ",
			expect: superSet, s1: "1011", s2: "1001", onChar: '1'},
		{name: "same length - superset ",
			expect: subSet, s1: "0011", s2: "0111", onChar: '1'},
		{name: "same length - not equal",
			expect: emptySet, s1: "101", s2: "01", onChar: '1'},
		{name: "2",
			expect: superSet, s1: "1011", s2: "101", onChar: '1'},
		{name: "2 - n",
			expect: emptySet, s1: "1010", s2: "010", onChar: '1'},
		{name: "3",
			expect: subSet, s1: "101", s2: "1011", onChar: '1'},
		{name: "3 - n",
			expect: emptySet, s1: "01", s2: "1011", onChar: '1'},
	}
	for _, tc := range tCases {
		got := charInclusion(tc.s1, tc.s2, tc.onChar)
		if got != tc.expect {
			t.Errorf("Test case %s failed. Expect: %v, got: %v", tc.name, tc.expect, got)
		}
	}
}

type nodeKeyer_Mock []string

func (n nodeKeyer_Mock) BaseKey() string {
	return n[0]
}
func (n nodeKeyer_Mock) LakeKey() string {
	return n[1]
}
func (n nodeKeyer_Mock) Key() string {
	return n[0] + n[1]
}

func Test_nodeInclusion(t *testing.T) {
	type tCase struct {
		name   string
		expect setType
		cg1    nodeKeyer_Mock
		cg2    nodeKeyer_Mock
	}
	tCases := []tCase{
		{
			name:   "full-full",
			expect: fullSet,
			cg1:    []string{"TTT", "111"},
			cg2:    []string{"TTT", "111"},
		},
		{
			name:   "none-none",
			expect: emptySet,
			cg1:    []string{"FTT", "1011"},
			cg2:    []string{"TFT", "1101"},
		},
		{
			name:   "full-superset",
			expect: superSet,
			cg1:    []string{"TTT", "1111"},
			cg2:    []string{"TTT", "1101"},
		},
		{
			name:   "full-subset",
			expect: subSet,
			cg1:    []string{"TTT", "1001"},
			cg2:    []string{"TTT", "1111"},
		},
		{
			name:   "full-none",
			expect: emptySet,
			cg1:    []string{"TTT", "1011"},
			cg2:    []string{"TTT", "1101"},
		},
		{
			name:   "superset-full",
			expect: superSet,
			cg1:    []string{"TTT", "1101"},
			cg2:    []string{"TFT", "1101"},
		},
		{
			name:   "subset-full",
			expect: subSet,
			cg1:    []string{"TFT", "1011"},
			cg2:    []string{"TTT", "1011"},
		},
		{
			name:   "none-full",
			expect: emptySet,
			cg1:    []string{"FTT", "0011"},
			cg2:    []string{"TFT", "0011"},
		},
		{
			name:   "subset-superset",
			expect: emptySet,
			cg1:    []string{"FTT", "1111"},
			cg2:    []string{"TTT", "1101"},
		},
		{
			name:   "superset-subset",
			expect: emptySet,
			cg1:    []string{"TTT", "1011"},
			cg2:    []string{"TFT", "1111"},
		},
	}

	for _, tc := range tCases {
		got := nodeInclusion(tc.cg1, tc.cg2)
		if got != tc.expect {
			t.Errorf("Test case %s failed. Expect: %v, got: %v", tc.name, tc.expect, got)
		}
	}
}

type nodeKeyerMap_Mock struct {
	nodeMap map[string]nodeKeyer_Mock
}

func (m nodeKeyerMap_Mock) Len() int {
	return len(m.nodeMap)
}
func (m nodeKeyerMap_Mock) ForEach(f func(string, NodeKeyer) bool) bool {
	for k, node := range m.nodeMap {
		if q := f(k, node); q {
			return q
		}
	}
	return false
}

func Test_findJointRelations(t *testing.T) {
	type tCase struct {
		name   string
		expect setType
		j1     nodeKeyerMap_Mock
		j2     nodeKeyerMap_Mock
	}
	tCases := []tCase{
		{
			name:   "1-full:1-full",
			expect: fullSet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"TT", "11"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"TT", "11"},
				},
			},
		},
		{
			name:   "1-subset:1-subset",
			expect: subSet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FT", "01"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"TT", "11"},
				},
			},
		},
		{
			name:   "1-superset:1-superset",
			expect: superSet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"TT", "11"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"0T", "01"},
				},
			},
		},
		{
			name:   "1-superset:1-subset",
			expect: emptySet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"TT", "01"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"0T", "11"},
				},
			},
		},
		{
			name:   "2-full:2-full",
			expect: fullSet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FFT", "001"},
					"k2": []string{"FTF", "100"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FFT", "001"},
					"k2": []string{"FTF", "100"},
				},
			},
		},
		{
			name:   "2-subset:2-subset",
			expect: subSet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FFT", "001"},
					"k2": []string{"FTF", "100"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FFT", "001"},
					"k2": []string{"TTF", "110"},
				},
			},
		},
		{
			name:   "2-subset:3-full",
			expect: subSet,
			j1: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FFT", "001"},
					"k2": []string{"FTF", "100"},
				},
			},
			j2: nodeKeyerMap_Mock{
				nodeMap: map[string]nodeKeyer_Mock{
					"k1": []string{"FFT", "001"},
					"k2": []string{"TTF", "100"},
					"k3": []string{"0FF", "010"},
				},
			},
		},
	}

	for _, tc := range tCases {
		got, _ := findJointRelations(tc.j1, tc.j2, true)
		if got != tc.expect {
			t.Errorf("Test case %s failed. Expect: %v, got: %v", tc.name, tc.expect, got)
		}
	}

}
*/