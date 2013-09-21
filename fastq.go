package fastq

import (
	"sort"
)

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

type Stream struct {
	summary []gksummary
	n       int
	b       int // block size
}

func New() *Stream {
	return &Stream{}
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

	sc := prune(s.summary[0], (s.b+1)/2+1)
	s.summary[0] = s.summary[0][:0] // empty

	for k := 1; k <= len(s.summary); k++ {
		// two versions of empty
		if s.summary[k] == nil {
			s.summary = append(s.summary, sc)
			return
		}

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

		tmp := s.merge(s.summary[k], sc)
		sc = prune(tmp, (s.b+1)/2+1)
		// NOTE: sc is used in next iteration
		// -  it is passed to the next level !

		s.summary[k] = s.summary[k][:0] // Re-initialize
	}
}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Prune"
func prune(sc gksummary, b int) gksummary {

	var r gksummary // result quantile summary

	for i := 0; i < b; i++ {
		rank := int(1.0 / float64(b) * float64(len(sc)))
		v := lookupRank(sc, rank)
		r = append(r, v) // add only if unique?
	}

	return r
}

// return the tuple containing rank 'r' in summary
func lookupRank(summary gksummary, r int) tuple {

	var rmin int

	n := len(summary)

	for _, t := range summary {
		rmin += t.g
		rmax := rmin + t.delta

		// FIXME: epsilon? 2*epsilon?
		if r-rmin <= int(epsilon*float64(n)) && rmax-r <= int(epsilon*float64(n)) {
			return t
		}
	}

	return tuple{}
}

// From http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald-D.html "Merge"
func (s *Stream) merge(s1, s2 []tuple) gksummary {

	var r []tuple

	var i1, i2 int

	// merge sort s1, s2 on 'v'
	for i1 < len(s1) && i2 < len(s2) {
		if s1[i1].v <= s2[i2].v {
			r = append(r, s1[i1])
			i1++
		} else {
			r = append(r, s2[i2])
			i2++
		}
	}

	// copy remaining entries from s1 or s2 (or neither)
	switch {
	case i1 < len(s1):
		r = append(r, s1[i1:]...)
	case i2 < len(s2):
		r = append(r, s2[i2:]...)
	}

	// TODO: assign rmin/rmax values

	// all done
	return r

}
