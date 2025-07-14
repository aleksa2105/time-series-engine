package page

import "encoding/binary"

type Metadata struct {
	MinValue uint64
	MaxValue uint64
	Count    uint64
}

func NewMetadata() *Metadata {
	return &Metadata{
		MinValue: ^uint64(0), // max uint64 (all bits are 1)
		MaxValue: 0,          // min uint64
		Count:    0,
	}
}

func (pm *Metadata) Serialize() []byte {
	allBytes := make([]byte, 0, 24)

	minValueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minValueBytes, pm.MinValue)
	allBytes = append(allBytes, minValueBytes...)

	maxValueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxValueBytes, pm.MaxValue)
	allBytes = append(allBytes, maxValueBytes...)

	countBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(countBytes, pm.Count)
	allBytes = append(allBytes, countBytes...)

	return allBytes
}
