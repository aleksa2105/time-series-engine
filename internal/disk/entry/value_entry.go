package entry

import (
	"encoding/binary"
	"math"
)

type ValueEntry struct {
	Value float64
}

func (ve *ValueEntry) Serialize() []byte {
	bits := math.Float64bits(ve.Value)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func DeserializeValueEntry(b []byte) *ValueEntry {
	bits := binary.BigEndian.Uint64(b)
	ve := &ValueEntry{}
	ve.Value = math.Float64frombits(bits)
	return ve
}

func (ve *ValueEntry) Size() uint64 {
	return uint64(len(ve.Serialize()))
}
