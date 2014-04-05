package fastq

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestMerge(t *testing.T) {

	return

	var tests = []struct {
		s1, s2 []tuple
		r      []tuple
		n1, n2 int
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
			8, 8,
		},
		/*
			{
				[]tuple{
					{1, 1, 1},
					{2, 1, 2},
					{3, 2, 1},
				},
				[]tuple{
					{1, 1, 1},
					{2, 1, 2},
					{3, 2, 1},
				},
				[]tuple{
					{1, 1, 2},
					{2, 2, 4},
					{3, 4, 2},
				},
			},

			{

				[]tuple{
					{5, 1, 0},
					{5, 1, 0},
					{5, 1, 0},
					{6, 1, 0},
				},

				[]tuple{
					{5, 1, 0},
					{5, 1, 0},
					{5, 1, 0},
				},
				[]tuple{},
			},
			{
				[]tuple{
					{5, 1, 2},
					{6, 3, 2},
					{7, 3, 4},
					{8, 5, 7},
					{9, 8, 2},
					{10, 3, 7},
					{11, 8, 3},
					{12, 4, 0},
					{15, 2, 0},
					{24, 2, 0},
					{26, 2, 1},
					{29, 2, 0},
					{30, 1, 2},
					{32, 3, 0},
					{36, 2, 0},
					{37, 1, 1},
					{42, 3, 4},
					{43, 5, 1},
				},
				[]tuple{
					{5, 1, 0},
					{7, 2, 1},
					{8, 2, 2},
					{9, 3, 6},
					{10, 7, 6},
					{11, 7, 3},
					{12, 4, 2},
					{15, 3, 1},
					{16, 2, 0},
					{21, 2, 3},
					{24, 4, 0},
					{29, 2, 0},
					{30, 1, 1},
					{32, 3, 0},
					{34, 2, 1},
					{35, 2, 0},
					{38, 2, 1},
					{39, 2, 1},
				},
				[]tuple{},
			},
		*/
	}

	for _, tst := range tests {

		r := merge(tst.s1, tst.s2, tst.n1, tst.n2)

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
		s1  []int
		b   int
		eps float64
		r   []tuple
	}{
		{
			[]int{1, 4, 7, 9, 11, 12, 13, 15},
			3,
			8.0 / 3.0,
			[]tuple{},
		},
	}

	for _, tst := range tests {

		var g gksummary

		for _, v := range tst.s1 {
			g = append(g, tuple{float64(v), 1, 0})
		}

		sort.Sort(&g)
		epsilon = tst.eps
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
