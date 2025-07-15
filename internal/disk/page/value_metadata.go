package page

import (
	"encoding/binary"
	"math"
)

type ValueMetadata struct {
	MinValue float64
	MaxValue float64
	Count    uint64
}

func NewValueMetadata() *ValueMetadata {
	return &ValueMetadata{
		MinValue: math.Inf(1),
		MaxValue: math.Inf(-1),
		Count:    0,
	}
}

func (vm *ValueMetadata) Serialize() []byte {
	allBytes := make([]byte, 0, 24)

	bits := math.Float64bits(vm.MinValue)
	minValueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minValueBytes, bits)
	allBytes = append(allBytes, minValueBytes...)

	bits = math.Float64bits(vm.MaxValue)
	maxValueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxValueBytes, bits)
	allBytes = append(allBytes, maxValueBytes...)

	countBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(countBytes, vm.Count)
	allBytes = append(allBytes, countBytes...)

	return allBytes
}
