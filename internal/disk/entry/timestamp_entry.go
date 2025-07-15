package entry

import (
	"encoding/binary"
)

type TimestampEntry struct {
	Value uint64
}

func (tse *TimestampEntry) Serialize() []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, tse.Value)
	return bytes
}

func DeserializeTimestampEntry(b []byte) *TimestampEntry {
	return &TimestampEntry{
		Value: binary.BigEndian.Uint64(b),
	}
}

func (tse *TimestampEntry) Size() uint64 {
	return uint64(len(tse.Serialize()))
}

func (tse *TimestampEntry) Delta(other *TimestampEntry) uint64 {
	return tse.Value - other.Value
}
