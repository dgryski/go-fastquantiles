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

func (s *Stream) Dump() {

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

			fmt.Println("setting", k, "to ", sc.Size())
			s.summary[k] = sc // Store it
			s.Dump()
			return // Done
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
	fmt.Println("fell off the end:", sc.Size())
	s.Dump()

}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Prune"
func prune(sc gksummary, b int) gksummary {

	var r gksummary // result quantile summary

	fmt.Printf("before prune: len(sc)=%d (n=%d) sc=%v\n", len(sc), sc.Size(), sc)

	v := lookupRank(sc, 1)
	elt := tuple{v: v.v, g: v.rmin, delta: v.rmax - 1}
	r = append(r, elt)

	rmin := elt.g
	for i := 1; i <= b; i++ {

		rank := int(float64(sc.Size()) * float64(i) / float64(b))
		v := lookupRank(sc, rank)

		elt := tuple{v: v.v}

		elt.g = v.rmin - rmin
		rmin += elt.g

		elt.delta = v.rmax - rmin

		if r != nil && r[len(r)-1].v == elt.v {
			// ignore if we've already seen it
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

	for i, t := range summary {
		rmin += t.g
		rmax := rmin + t.delta

		if i+1 == len(summary) {
			return lookupResult{v: t.v, rmin: rmin, rmax: rmax}

		}

		rmin_next := rmin + summary[i+1].g

		// this is not entirely right
		if rmin <= r && r <= rmin_next {
			return lookupResult{v: t.v, rmin: rmin, rmax: rmax}
		}
	}

	panic("not found")
}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Merge"
// or "COMBINE" in http://www.cs.umd.edu/~samir/498/kh.pdf
func merge(s1, s2 gksummary) gksummary {

	fmt.Printf("before merge: len(s1)=%d (n=%d) s1=%v\n", s1.Len(), s1.Size(), s1)
	fmt.Printf("before merge: len(s2)=%d (n=%d) s2=%v\n", s2.Len(), s2.Size(), s2)

	if len(s1) == 0 {
		return s2
	}
	if len(s2) == 0 {
		return s1
	}

	var r gksummary

	var i1, i2 int

	rmin1 := 0
	rmin2 := 0

	var rmax1, rmax2 int

	rmin := 0
	// merge sort s1, s2 on 'v'
	for i1 < len(s1) && i2 < len(s2) {

		// This section is very tricky because the papers and course notes
		// talk in terms of r_min and r_max, but the data structure
		// contains g and delta which let you _calculate_ r_min and r_max

		if s1[i1].v == s2[i2].v {
			s1[i1].delta += s2[i2].delta
			if i2+1 < len(s2) {
				s2[i2+1].g += s2[i2].g
			}
			s2[i2].g = 0 // mark as skip
			i2++
			continue
		}

		// ugg, these two blocks are going to get out of sync..
		if s1[i1].v < s2[i2].v {

			// rmin/rmax of s1[i1].v
			rmin1 += s1[i1].g
			rmax1 = rmin1 + s1[i1].delta

			// use notation from paper
			xr := s1[i1]
			xrRmin := rmin1
			xrRmax := xrRmin + xr.delta

			zi := tuple{v: xr.v}

			// find y_s with y_s < x_r
			ysIdx := i2 - 1 // must start at i2-1, since if s2[i2] was smaller it would have been processed already
			ysRmin := rmin2 // rmin2 is sum(s2[0:i2]), so == rmin(s2[ysIdx])
			for ysIdx >= 0 && s2[ysIdx].g == 0 {
				ysIdx--
			}

			var ziRmin int
			if ysIdx >= 0 {
				ziRmin = xrRmin + ysRmin
			} else {
				ziRmin = xrRmin
			}

			ytIdx := i2
			ytRmin := rmin2 + s2[ytIdx].g

			var ziRmax int
			ytRmax := ytRmin + s2[ytIdx].delta
			ziRmax = xrRmax + ytRmax - 1

			zi.delta = ziRmax - ziRmin
			zi.g = ziRmin - rmin

			rmin += zi.g
			r = append(r, zi)

			i1++

		} else if s2[i2].v < s1[i1].v {

			// rmin/rmax of s2[i1].v (current element)
			rmin2 += s2[i2].g
			rmax2 = rmin2 + s2[i2].delta

			// use notation from paper
			xr := s2[i2]
			xrRmin := rmin2
			xrRmax := xrRmin + xr.delta

			zi := tuple{v: xr.v}

			// find y_s with y_s < x_r
			ysIdx := i1 - 1 // must start at i1-1, since if s1[i1] was smaller it would have been processed already
			ysRmin := rmin1 // rmin1 is sum(s1[0:i1]), so == rmin(s1[ysIdx])

			var ziRmin int
			if ysIdx >= 0 {
				ziRmin = xrRmin + ysRmin
			} else {
				ziRmin = xrRmin
			}

			ytIdx := i1
			ytRmin := rmin1 + s1[ytIdx].g

			var ziRmax int
			ytRmax := ytRmin + s1[ytIdx].delta
			ziRmax = xrRmax + ytRmax - 1

			zi.delta = ziRmax - ziRmin
			zi.g = ziRmin - rmin

			rmin += zi.g
			r = append(r, zi)

			i2++
		}
	}

	// only one of these for-loops will ever happen
	// FIXME: combine into single routine somehow (aliasing..)

	for ; i1 < len(s1); i1++ {
		elt := s1[i1]
		rmin1 += elt.g
		rmax1 := rmin1 + elt.delta

		elt.g = rmin1 + rmin2 - rmin
		rmin += elt.g

		elt.delta = (rmax1 + rmax2) - rmin

		r = append(r, elt)

		i1++
	}

	for ; i2 < len(s2); i2++ {
		elt := s2[i2]
		rmin2 += elt.g
		rmax2 := rmin2 + elt.delta

		elt.g = rmin2 + rmin1 - rmin
		rmin += elt.g

		elt.delta = (rmax2 + rmax1) - rmin

		r = append(r, elt)

		i2++
	}

	// all done
	fmt.Printf(" after merge : len(r)=%d (n=%d) r=%v\n", r.Len(), r.Size(), r)
	r.mergeValues()
	//	fmt.Printf(" after mergev: len(r)=%d (n=%d) r=%v\n", r.Len(), r.Size(), r)
	return r
}

// !! Must call Finish to allow processing queries
func (s *Stream) Finish() {
	fmt.Println("Finish")
	sort.Sort(&s.summary[0])
	s.summary[0].mergeValues()

	s.Dump()

	fmt.Println("size[0]=", s.summary[0].Size())

	for i := 1; i < len(s.summary); i++ {
		s.summary[0] = merge(s.summary[0], s.summary[i])
		fmt.Printf("merged %d: size=%d\n", i, s.summary[0].Size())
	}
}

// GK query
func (s *Stream) Query(q float64) float64 {

	// convert quantile to rank

	r := int(q * float64(s.n))

	fmt.Println("querying rank=", r, "of", s.n, "items")
	fmt.Println("querying s0.Size()=", s.summary[0].Size())

	var rmin int

	for i, t := range s.summary[0] {

		if i+1 == len(s.summary[0]) {
			return t.v
		}

		rmin += t.g
		rmin_next := rmin + s.summary[0][i+1].g

		if rmin <= r && r <= rmin_next {
			return t.v
		}
	}

	panic("not reached")
}
