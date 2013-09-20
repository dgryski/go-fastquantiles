// Package gk implements Greenwald-Khanna streaming quantiles
// http://dl.acm.org/citation.cfm?doid=375663.375670
// http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald.html
// implementation translated from http://papercruncher.com/2010/03/02/stream-algorithms-order-statistics/
package gk

import (
	"container/list"
	"math"
)

const epsilon = 0.01

type tuple struct {
	v     float64
	g     float64
	delta float64
}

type Stream struct {
	summary *list.List
	n       int
}

func New() *Stream {
	return &Stream{summary: list.New()}
}

func (s *Stream) Insert(v float64) {

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
	s.compress()
}

func (s *Stream) compress() {

	for elt := s.summary.Front(); elt.Next() != nil; {
		next := elt.Next()
		t := elt.Value.(*tuple)
		nt := next.Value.(*tuple)
		if t.g+nt.g+nt.delta <= math.Floor(2*epsilon*float64(s.n)) {
			nt.g += t.g
			s.summary.Remove(elt)
		}
		elt = next
	}
}

func (s *Stream) Query(q float64) float64 {

	// convert quantile to rank

	r := q * float64(s.n)

	var rmin float64

	for elt := s.summary.Front(); elt.Next() != nil; elt = elt.Next() {

		t := elt.Value.(*tuple)

		rmin += t.g
		rmax := rmin + t.delta

		if r-rmin <= epsilon*float64(s.n) && rmax-r <= epsilon*float64(s.n) {
			return t.v
		}
	}

	// panic("not reached")

	return 0
}
