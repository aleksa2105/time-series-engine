package entry

import (
	"encoding/binary"
	"math"
)

type ValueEntry struct {
	Value          float64
	CompressedData *ValCompressedData
}

func NewValueEntry(value float64, compressedData *ValCompressedData) *ValueEntry {
	return &ValueEntry{
		Value:          value,
		CompressedData: compressedData,
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
	if ve.CompressedData.Compressed == true {
		return uint64(ve.CompressedData.ValueSize)
	}
	return uint64(ve.CompressedData.ValueSize) + 2
}

func (ve *ValueEntry) GetValue() uint64 {
	return math.Float64bits(ve.Value)
}
