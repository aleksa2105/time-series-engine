package entry

import (
	"encoding/binary"
	"math"
)

type ValueEntry struct {
	Value          uint64
	CompressedData *CompressedData
}

func NewValueEntry(value float64) *ValueEntry {
	return &ValueEntry{
		Value:          math.Float64bits(value),
		CompressedData: nil,
	}
}

func (ve *ValueEntry) Serialize() []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, ve.Value)
	return bytes
}

func DeserializeValueEntry(b []byte) *ValueEntry {
	return &ValueEntry{
		Value: binary.BigEndian.Uint64(b),
	}
}

func (ve *ValueEntry) Size() uint64 {
	return uint64(ve.CompressedData.ValueSize)
}
