package fastq

import (
	"fmt"
	"math"
	"sort"
)

var _ = fmt.Println

var epsilon = 0.000001

const debug = false

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

	l := len(*gk)

	if l == 0 {
		return 0
	}

	var n int
	for _, t := range *gk {
		n += t.g
	}

	return n + (*gk)[l-1].delta

}

// reduces the number of elements but doesn't lose precision
// value merging: from Appendig A of http://www.cis.upenn.edu/~mbgreen/papers/pods04.pdf
func (gk *gksummary) mergeValues() {

	if debug {
		fmt.Println("before: size=", gk.Size(), gk)
	}

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

	if debug {
		fmt.Println(" after: size=", gk.Size(), gk)
	}
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

func (s *Stream) Dump() {

	if !debug {
		return
	}

	fmt.Printf("stream size: %d\n", s.n)
	for i, sl := range s.summary {
		fmt.Printf("summary[%d]=%d\n", i, sl.Size())
	}

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

			if debug {
				fmt.Println("setting", k, "to ", sc.Size())
			}
			s.summary[k] = sc // Store it
			s.Dump()
			return // Done
		}

		/* --------------------------------------
		   sk contained a compressed summary
		   -------------------------------------- */

		tmp := merge(s.summary[k], sc, s.b*(1<<uint(k)), s.b*(1<<uint(k))) // here we're merging two summaries with s.b * 2^k entries each
		sc = prune(tmp, (s.b+1)/2+1)
		// NOTE: sc is used in next iteration
		// -  it is passed to the next level !

		s.summary[k] = s.summary[k][:0] // Re-initialize
	}

	// fell off the end of our loop -- no more s.summary entries
	s.summary = append(s.summary, sc)
	if debug {
		fmt.Println("fell off the end:", sc.Size())
	}
	s.Dump()

}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Prune"
func prune(sc gksummary, b int) gksummary {

	var r gksummary // result quantile summary

	if debug {
		fmt.Printf("before prune: len(sc)=%d (n=%d) sc=%v\n", len(sc), sc.Size(), sc)
	}

	elt := sc[0]
	r = append(r, elt)

	scIdx := 0
	scRmin := 1
	for i := 1; i <= b; i++ {

		rank := int(float64(sc.Size()) * float64(i) / float64(b))

		// find an element of rank 'rank' in sc
		for scIdx < len(sc)-1 {

			if scRmin <= rank && rank < scRmin+sc[scIdx+1].g {
				break
			}

			scIdx++
			scRmin += sc[scIdx].g
		}

		if scIdx >= len(sc) {
			scIdx = len(sc) - 1
		}

		elt := sc[scIdx]

		if r != nil && r[len(r)-1].v == elt.v {
			// ignore if we've already seen it
			continue
		}

		r = append(r, elt)
	}

	if debug {
		fmt.Printf(" after prune : len(r)=%d (n=%d) r= %v\n", r.Len(), r.Size(), r)
	}
	return r
}

// This is the Merge algorithm from
// http://www.cs.ubc.ca/~xujian/paper/quant.pdf .  It is much simpler than the
// MERGE algorithm at
// http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html
// or "COMBINE" in http://www.cs.umd.edu/~samir/498/kh.pdf .
func merge(s1, s2 gksummary, N1, N2 int) gksummary {

	if debug {
		fmt.Printf("before merge: len(s1)=%d (n=%d) s1=%v\n", s1.Len(), s1.Size(), s1)
		fmt.Printf("before merge: len(s2)=%d (n=%d) s2=%v\n", s2.Len(), s2.Size(), s2)
	}

	if len(s1) == 0 {
		return s2
	}
	if len(s2) == 0 {
		return s1
	}

	var smerge gksummary

	var i1, i2 int

	rmin := 0
	k := 0

	s1[0].g = 1
	s2[0].g = 1

	for i1 < len(s1) || i2 < len(s2) {

		var t tuple

		if i1 < len(s1) && i2 < len(s2) {

			if s1[i1].v <= s2[i2].v {
				t = s1[i1]
				i1++
			} else {
				t = s2[i2]
				i2++
			}
		} else if i1 < len(s1) && i2 >= len(s2) {
			t = s1[i1]
			i1++
		} else if i1 >= len(s1) && i2 < len(s2) {
			t = s2[i2]
			i2++
		} else {
			panic("invariant violated")
		}

		newt := tuple{v: t.v, g: t.g}

		k++
		// If you're following along with the paper, the Algorithm has
		// a typo on lines 9 and 11.  The summation is listed as going
		// from 1..k , which doesn't make any sense.  It should be
		// 1..l, the number of summaries we're merging.  In this case,
		// l=2, so we just add the sizes of the sets.
		if k == 1 {
			newt.delta = int(epsilon * (float64(N1) + float64(N2)))
		} else {
			newt.delta = rmin + int(2*epsilon*(float64(N1)+float64(N2)))
			rmin += newt.g
			smerge = append(smerge, newt)
		}
	}

	// all done

	if debug {
		fmt.Printf(" after merge : len(r)=%d (n=%d) r=%v\n", smerge.Len(), smerge.Size(), smerge)
	}

	// The merged list might have duplicate elements -- merge them.
	smerge.mergeValues()

	return smerge
}

// !! Must call Finish to allow processing queries
func (s *Stream) Finish() {
	if debug {
		fmt.Println("Finish")
	}
	sort.Sort(&s.summary[0])
	s.summary[0].mergeValues()

	s.Dump()

	if debug {
		fmt.Println("size[0]=", s.summary[0].Size())
	}

	size := len(s.summary[0])

	for i := 1; i < len(s.summary); i++ {
		if debug {
			fmt.Printf("merging: %v\n", s.summary[i])
		}
		// FIXME(dgryski): hrm, merging two summaries with unequal elements here .. ?

		s.summary[0] = merge(s.summary[0], s.summary[i], size, s.b*1<<uint(i))
		size += s.b * 1 << uint(i)
		if debug {
			fmt.Printf("merged %d: size=%d\n", i, s.summary[0].Size())
		}
	}
}

// GK query
func (s *Stream) Query(q float64) float64 {

	// convert quantile to rank

	r := int(q * float64(s.n))

	if debug {
		fmt.Println("querying rank=", r, "of", s.n, "items")
		fmt.Println("querying s0.Size()=", s.summary[0].Size())
	}

	var rmin int

	for i, t := range s.summary[0] {

		if i+1 == len(s.summary[0]) {
			return t.v
		}

		rmin += t.g
		rmin_next := rmin + s.summary[0][i+1].g

		if rmin <= r && r < rmin_next {
			return t.v
		}
	}

	panic("not reached")
}
