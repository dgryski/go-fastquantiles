package fastq

import (
	"fmt"
	"testing"
)

func TestMerge(t *testing.T) {

	var tests = []struct {
		s1, s2 []tuple
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
		},

		{

			[]tuple{
				{5, 1, 0},
				{5, 1, 0},
				{5, 1, 0},
			},

			[]tuple{
				{5, 1, 0},
				{5, 1, 0},
				{5, 1, 0},
			},
		},
	}

	for _, tst := range tests {

		r := merge(tst.s1, tst.s2)

		rmin := 0

		for _, e := range r {
			rmin += e.g
			rmax := rmin + e.delta
			fmt.Printf("%d:[%d..%d]\n", int(e.v), rmin, rmax)
		}
	}

}
