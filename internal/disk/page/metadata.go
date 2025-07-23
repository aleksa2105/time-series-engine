package page

import "encoding/binary"

const MetadataSize uint64 = 24

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

func (md *Metadata) UpdateMinMaxValue(value uint64) {
	if md.MinValue > value {
		md.MinValue = value
	}
	if md.MaxValue < value {
		md.MaxValue = value
	}
}

func (md *Metadata) Serialize() []byte {
	allBytes := make([]byte, MetadataSize)
	binary.BigEndian.PutUint64(allBytes[0:8], md.MinValue)
	binary.BigEndian.PutUint64(allBytes[8:16], md.MaxValue)
	binary.BigEndian.PutUint64(allBytes[16:24], md.Count)
	return allBytes
}

func DeserializeMetadata(bytes []byte) *Metadata {
	if uint64(len(bytes)) < MetadataSize {
		return nil
	}

	minValue := binary.BigEndian.Uint64(bytes[:8])
	maxValue := binary.BigEndian.Uint64(bytes[8:16])
	count := binary.BigEndian.Uint64(bytes[16:24])
	return &Metadata{
		MinValue: minValue,
		MaxValue: maxValue,
		Count:    count,
	}
}
