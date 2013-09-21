package fastq

import (
	"container/list"
	"math"
)

//const epsilon = 0.01

type gktuple struct {
	v     float64
	g     float64
	delta float64
}

type GKStream struct {
	summary *list.List
	n       int
}

func NewGK() *GKStream {
	return &GKStream{summary: list.New()}
}

func (s *GKStream) Insert(v float64) {

	value := &gktuple{v, 1, 0}

	var idx int
	var elt *list.Element

	for elt = s.summary.Front(); elt != nil; elt = elt.Next() {
		t := elt.Value.(*gktuple)
		if v < t.v {
			break
		}
		idx++
	}

	if idx == 0 || idx == s.summary.Len() {
		// the new element is the new min or max
		value.delta = 0
	} else {
		value.delta = math.Floor(2 * epsilon * float64(s.n))
	}

	if idx == 0 {
		s.summary.PushFront(value)
	} else if elt == nil {
		s.summary.PushBack(value)
	} else {
		s.summary.InsertBefore(value, elt)
	}

	s.n++
	if s.n%int(1.0/float64(2.0*epsilon)) == 0 {
		s.compress()
	}
}

func (s *GKStream) compress() {

	for elt := s.summary.Front(); elt.Next() != nil; {
		next := elt.Next()
		t := elt.Value.(*gktuple)
		nt := next.Value.(*gktuple)
		if t.g+nt.g+nt.delta <= math.Floor(2*epsilon*float64(s.n)) {
			nt.g += t.g
			s.summary.Remove(elt)
		}
		elt = next
	}
}

func (s *GKStream) Query(q float64) float64 {

	// convert quantile to rank

	r := q * float64(s.n)

	var rmin float64

	for elt := s.summary.Front(); elt.Next() != nil; elt = elt.Next() {

		t := elt.Value.(*gktuple)

		rmin += t.g
		rmax := rmin + t.delta

		if r-rmin <= epsilon*float64(s.n) && rmax-r <= epsilon*float64(s.n) {
			return t.v
		}
	}

	// panic("not reached")

	return 0
}
