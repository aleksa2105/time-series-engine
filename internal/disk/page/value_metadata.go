package page

import (
	"encoding/binary"
)

type ValueMetadata struct {
	MinValue uint64
	MaxValue uint64
	Count    uint64
}

func NewValueMetadata() *ValueMetadata {
	return &ValueMetadata{
		MinValue: ^uint64(0), // max uint64 (all bits are 1)
		MaxValue: 0,          // min uint64
		Count:    0,
	}
}

func (vmd *ValueMetadata) UpdateMinMaxValue(value uint64) {
	if vmd.MinValue > value {
		vmd.MinValue = value
	}
	if vmd.MaxValue < value {
		vmd.MaxValue = value
	}
}

func (vmd *ValueMetadata) Serialize() []byte {
	allBytes := make([]byte, MetadataSize)
	binary.BigEndian.PutUint64(allBytes[0:8], vmd.MinValue)
	binary.BigEndian.PutUint64(allBytes[8:16], vmd.MaxValue)
	binary.BigEndian.PutUint64(allBytes[16:24], vmd.Count)
	return allBytes
}
