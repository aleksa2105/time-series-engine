package entry

import (
	"encoding/binary"
)

type TimestampEntry struct {
	Value uint64
}

func NewTimestampEntry(value uint64) *TimestampEntry {
	return &TimestampEntry{
		Value: value,
	}
}

func (tse *TimestampEntry) Serialize() []byte {
	bytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bytes, tse.Value)
	if n <= 0 {
		return nil
	}
	return bytes[:n]
}

func DeserializeTimestampEntry(b []byte) (*TimestampEntry, uint64) {
	value, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, 0
	}

	return &TimestampEntry{
		Value: value,
	}, uint64(n)
}

func (tse *TimestampEntry) Size() uint64 {
	return uint64(len(tse.Serialize()))
}
