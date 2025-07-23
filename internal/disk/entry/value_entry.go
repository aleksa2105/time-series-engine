package entry

import (
	"encoding/binary"
	"math"
)

type ValueEntry struct {
	Value          float64
	CompressedData *CompressedData
}

func NewValueEntry(value float64) *ValueEntry {
	return &ValueEntry{
		Value:          value,
		CompressedData: nil,
	}
}

func (ve *ValueEntry) Serialize() []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, math.Float64bits(ve.Value))
	return bytes
}

func DeserializeValueEntry(b []byte) *ValueEntry {
	return &ValueEntry{
		Value: float64(binary.BigEndian.Uint64(b)),
	}
}

func (ve *ValueEntry) Size() uint64 {
	return uint64(ve.CompressedData.ValueSize)
}
