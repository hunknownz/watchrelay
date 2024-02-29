package watchrelay

import "sync/atomic"

// Sequence is a simple counter that can be used to generate unique IDs.
type Sequence struct {
	value uint64
}

// NewSequence creates a new Sequence with the given start value.
func NewSequence(start uint64) *Sequence {
	return &Sequence{value: start}
}

// Next returns the next value in the sequence.
func (s *Sequence) Next() uint64 {
	return atomic.AddUint64(&s.value, 1)
}

// Current returns the current value in the sequence.
func (s *Sequence) Current() uint64 {
	return atomic.LoadUint64(&s.value)
}
