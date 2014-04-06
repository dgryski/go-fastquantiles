package fastquantiles

import (
	"container/list"
	"math"
)

type GKStream struct {
	summary *list.List
	n       int
}

func NewGK() *GKStream {
	return &GKStream{summary: list.New()}
}

func (s *GKStream) Insert(v float64) {

	value := &tuple{v, 1, 0}

	var idx int
	var elt *list.Element

	for elt = s.summary.Front(); elt != nil; elt = elt.Next() {
		t := elt.Value.(*tuple)
		if v < t.v {
			break
		}
		idx++
	}

	if idx == 0 || idx == s.summary.Len() {
		// the new element is the new min or max
		value.delta = 0
	} else {
		value.delta = int(math.Floor(2 * epsilon * float64(s.n)))
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
		t := elt.Value.(*tuple)
		nt := next.Value.(*tuple)
		if t.g+nt.g+nt.delta <= int(math.Floor(2*epsilon*float64(s.n))) {
			nt.g += t.g
			s.summary.Remove(elt)
		}
		elt = next
	}
}

func (s *GKStream) Query(q float64) float64 {

	// convert quantile to rank

	r := int(q * float64(s.n))

	var rmin int

	for elt := s.summary.Front(); elt.Next() != nil; elt = elt.Next() {

		t := elt.Value.(*tuple)

		rmin += t.g
		rmax := rmin + t.delta

		if r-rmin <= int(epsilon*float64(s.n)) && rmax-r <= int(epsilon*float64(s.n)) {
			return t.v
		}
	}

	// panic("not reached")

	return 0
}
