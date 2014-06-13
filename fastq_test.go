package fastquantiles

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestMerge(t *testing.T) {

	var tests = []struct {
		s1, s2  []tuple
		r       []tuple
		epsilon float64
		n1, n2  int
	}{
		{
			//   Q'  = { 2:[1..1], 4:[3..4], 8:[5..6], 17:[8..8] }
			[]tuple{
				{2, 1, 0},
				{4, 2, 1},
				{8, 2, 1},
				{17, 3, 0},
			},

			//   Q'' = { 1:[1..1], 7:[3..3], 12:[5..6], 15:[8..8] }
			[]tuple{
				{1, 1, 0},
				{7, 2, 0},
				{12, 2, 1},
				{15, 3, 0},
			},
			[]tuple{
				{1, 1, 0},
				{2, 1, 1},
				{4, 2, 2},
				{7, 2, 2},
				{8, 2, 3},
				{12, 2, 3},
				{15, 3, 2},
				{17, 3, 0},
			},
			0.375,
			8, 8,
		},
	}

	for _, tst := range tests {

		r := merge(tst.s1, tst.s2, tst.epsilon, tst.n1, tst.n2)

		if !reflect.DeepEqual(tst.r, r) {
			rmin := 0
			for _, e := range r {
				rmin += e.g
				rmax := rmin + e.delta
				fmt.Printf("%d:[%d..%d]\n", int(e.v), rmin, rmax)
			}
			t.Error("Failed: got r=", r, "\n\t\twanted r=", tst.r)
		}
	}

}

func TestPrune(t *testing.T) {

	var tests = []struct {
		s1 []int
		b  int
		r  []tuple
	}{
		{
			[]int{1, 4, 7, 9, 11, 12, 13, 15},
			3,
			[]tuple{},
		},
	}

	for _, tst := range tests {

		var g gksummary

		for _, v := range tst.s1 {
			g = append(g, tuple{float64(v), 1, 0})
		}

		sort.Sort(&g)
		(&g).mergeValues()

		r := prune(g, tst.b)

		if !reflect.DeepEqual(tst.r, r) {
			rmin := 0
			for _, e := range r {
				rmin += e.g
				rmax := rmin + e.delta
				fmt.Printf("%d:[%d..%d]\n", int(e.v), rmin, rmax)
			}
			t.Error("Failed: got r=", r, "\n\t\twanted r=", tst.r)
		}
	}
}
