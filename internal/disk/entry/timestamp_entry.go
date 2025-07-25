package entry

import (
	"encoding/binary"
)

type TimestampEntry struct {
	Value          uint64
	CompressedData *TSCompressedData
}

func NewTimestampEntry(value uint64, compressedData *TSCompressedData) *TimestampEntry {
	return &TimestampEntry{
		Value:          value,
		CompressedData: compressedData,
	}
}

func (tse *TimestampEntry) Serialize() []byte {
	return tse.CompressedData.Bytes
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
	return uint64(len(tse.CompressedData.Bytes))
}

func (tse *TimestampEntry) GetValue() uint64 {
	return tse.Value
}
