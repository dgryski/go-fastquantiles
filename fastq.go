package fastq

import (
	"fmt"
	"math"
	"sort"
)

var _ = fmt.Println

const epsilon = 0.01

type tuple struct {
	v     float64
	g     int
	delta int
}

type gksummary []tuple

func (gk *gksummary) Len() int           { return len(*gk) }
func (gk *gksummary) Less(i, j int) bool { return (*gk)[i].v < (*gk)[j].v }
func (gk *gksummary) Swap(i, j int)      { (*gk)[i], (*gk)[j] = (*gk)[j], (*gk)[i] }

func (gk *gksummary) Size() int {

	var n int

	for _, t := range *gk {
		n += t.g
	}

	return n

}

// reduces the number of elements but doesn't lose precision
// value merging: from Appendig A of http://www.cis.upenn.edu/~mbgreen/papers/pods04.pdf
func (gk *gksummary) mergeValues() {

	fmt.Println("before: size=", gk.Size(), gk)

	var missing int

	var dst int

	for src := 1; src < len(*gk); src++ {
		if (*gk)[dst].v == (*gk)[src].v {
			(*gk)[dst].delta += (*gk)[src].g + (*gk)[src].delta
			missing += (*gk)[src].g
			continue
		}

		dst++
		// add in the extra 'g' for the elements we removed
		(*gk)[src].g += missing
		missing = 0
		(*gk)[dst] = (*gk)[src]
	}

	(*gk) = (*gk)[:dst+1]

	fmt.Println(" after: size=", gk.Size(), gk)
}

type Stream struct {
	summary []gksummary
	n       int
	b       int // block size
}

func New(n int) *Stream {
	b := int(math.Floor(math.Log(epsilon*float64(n)) / epsilon))
	return &Stream{summary: make([]gksummary, 1, 1), n: n, b: b}
}

func (s *Stream) Update(e float64) {

	// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Zhang.html

	s.summary[0] = append(s.summary[0], tuple{e, 1, 0}) // insert unsorted

	if len(s.summary[0]) < s.b {
		return // all done
	}

	/* -----------------------------------
	   Level 0 is full... PACK IT UP !!!
	   ----------------------------------- */

	sort.Sort(&s.summary[0])

	s.summary[0].mergeValues()

	sc := prune(s.summary[0], (s.b+1)/2+1)
	s.summary[0] = s.summary[0][:0] // empty

	for k := 1; k < len(s.summary); k++ {

		if len(s.summary[k]) == 0 {
			/* --------------------------------------
			   Empty: put compressed summary in sk
			   -------------------------------------- */

			s.summary[k] = sc // Store it
			return            // Done
		}

		/* --------------------------------------
		   sk contained a compressed summary
		   -------------------------------------- */

		tmp := merge(s.summary[k], sc)
		sc = prune(tmp, (s.b+1)/2+1)
		// NOTE: sc is used in next iteration
		// -  it is passed to the next level !

		s.summary[k] = s.summary[k][:0] // Re-initialize
	}

	// fell off the end of our loop -- no more s.summary entries
	s.summary = append(s.summary, sc)

}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Prune"
func prune(sc gksummary, b int) gksummary {

	var r gksummary // result quantile summary

	fmt.Printf("before prune: len(sc)=%d (n=%d) sc=%v\n", len(sc), sc.Size(), sc)

	rmin := 0

	for i := 0; i < (b + 1); i++ {
		rank := int(float64(sc.Size()) * float64(i) / float64(b))
		v := lookupRank(sc, rank)

		elt := tuple{v: v.v}

		elt.g = v.rmin - rmin
		rmin += elt.g

		elt.delta = v.rmax - rmin

		// not sure if this is right. merge or ignore?
		if r != nil && r[len(r)-1].v == elt.v {
			e := r[len(r)-1]
			e.delta += elt.g + elt.delta
			r[len(r)-1] = e
			continue
		}

		r = append(r, elt)
	}

	fmt.Printf(" after prune : len(r)=%d (n=%d) r=%v\n", r.Len(), r.Size(), r)
	return r
}

type lookupResult struct {
	v    float64
	rmin int
	rmax int
}

// return the tuple containing rank 'r' in summary
// combine this inline with prune(), otherwise we're O(n^2)
// or over a channel?
func lookupRank(summary gksummary, r int) lookupResult {

	var rmin int

	n := len(summary)

	for _, t := range summary {
		rmin += t.g
		rmax := rmin + t.delta

		// FIXME: epsilon? 2*epsilon?
		if r-rmin <= int(epsilon*float64(n)) && rmax-r <= int(epsilon*float64(n)) {
			return lookupResult{t.v, rmin, rmax}
		}
	}

	return lookupResult{}
}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Merge"
// or "COMBINE" in http://www.cs.umd.edu/~samir/498/kh.pdf
func merge(s1, s2 gksummary) gksummary {

	fmt.Printf("before merge: len(s1)=%d (n=%d) s1=%v\n", s1.Len(), s1.Size(), s1)
	fmt.Printf("before merge: len(s2)=%d (n=%d) s2=%v\n", s2.Len(), s2.Size(), s2)

	var r gksummary

	var i1, i2 int

	rmin1 := 0
	rmax1 := 1
	rmin2 := 0
	rmax2 := 1

	rmin := 0
	// merge sort s1, s2 on 'v'
	for i1 < len(s1) && i2 < len(s2) {

		// This section is very tricky because the papers and course notes
		// talk in terms of r_min and r_max, but the data structure
		// contains g and delta which let you _calculate_ r_min and r_max

		if s1[i1].v <= s2[i2].v {

			elt := s1[i1]
			rmin1 += elt.g
			rmax1 = rmin1 + elt.delta

			if rmin2 != 0 {
				elt.g = rmin1 + rmin2 - rmin
			} else {
				elt.g = rmin1 - rmin
			}
			if elt.g < 0 {
				panic("s1 g < 0")
			}
			rmin += elt.g

			rmaxyt := rmin2 + s2[i2].g + s2[i2].delta

			elt.delta = (rmax1 + rmaxyt - 1) - rmin

			if elt.delta < 0 {
				fmt.Printf("yt: %d + %d + %d = %d\n", rmin2, s2[i2].g, s2[i2].delta, rmaxyt)
				fmt.Printf("d: %d + %d -1 - %d = %d\n", rmax1, rmaxyt, rmin, elt.delta)
				panic("s1 delta < 0")

			}
			r = append(r, elt)

			i1++
		} else {

			elt := s2[i2]
			rmin2 += elt.g
			rmax2 = rmin2 + elt.delta

			if rmin1 != 0 {
				elt.g = rmin2 + rmin1 - rmin
			} else {
				elt.g = rmin2 - rmin
			}
			if elt.g < 0 {
				panic("s2 g < 0")
			}

			rmin += elt.g

			rmaxyt := rmin1 + s1[i1].g + s1[i1].delta

			elt.delta = (rmax2 + rmaxyt - 1) - rmin

			if elt.delta < 0 {
				fmt.Printf("yt: %d + %d + %d = %d\n", rmin1, s1[i1].g, s1[i1].delta, rmaxyt)
				fmt.Printf("d: %d + %d -1 - %d = %d\n", rmax2, rmaxyt, rmin, elt.delta)
				panic("s2 delta < 0")
			}
			r = append(r, elt)

			i2++
		}
	}

	// only one of these for-loops will ever happen
	// FIXME: combine into single routine somehow (aliasing..)

	for ; i1 < len(s1); i1++ {
		elt := s1[i1]
		rmin1 += elt.g
		rmax1 = rmin1 + elt.delta

		elt.g = rmin1 + rmin2 - rmin
		rmin += elt.g

		elt.delta = (rmax1 + rmax2) - rmin

		r = append(r, elt)

		i1++
	}

	for ; i2 < len(s2); i2++ {
		elt := s2[i2]
		rmin2 += elt.g
		rmax2 = rmin2 + elt.delta

		elt.g = rmin2 + rmin1 - rmin
		rmin += elt.g

		elt.delta = (rmax2 + rmax1) - rmin

		r = append(r, elt)

		i2++
	}

	// all done
	fmt.Printf(" after merge : len(r)=%d (n=%d) r=%v\n", r.Len(), r.Size(), r)
	return r
}

// !! Must call Finish to allow processing queries
func (s *Stream) Finish() {
	S := s.summary[0]

	sort.Sort(&s.summary[0])

	for i := 1; i < len(s.summary); i++ {
		S = merge(S, s.summary[i])
	}

	s.summary[0] = S
}

// GK query
func (s *Stream) Query(q float64) float64 {

	// convert quantile to rank

	r := int(q * float64(s.n))

	var rmin int

	for _, t := range s.summary[0] {

		rmin += t.g
		rmax := rmin + t.delta

		if r-rmin <= int(epsilon*float64(s.n)) && rmax-r <= int(epsilon*float64(s.n)) {
			return t.v
		}
	}

	// panic("not reached")

	return 0

}
