package row_group

import (
	"encoding/binary"
	"errors"
	"math"
	"time-series-engine/internal"
)

type Metadata struct {
	MinTimestamp uint64
	MaxTimestamp uint64

	MinValue float64
	MaxValue float64
}

func NewMetadata() *Metadata {
	return &Metadata{
		MinTimestamp: ^uint64(0), // max uint64 (all bits are 1)
		MaxTimestamp: 0,

		MinValue: math.Inf(1),
		MaxValue: math.Inf(-1),
	}
}

func (m *Metadata) Update(p *internal.Point) {
	if p.Timestamp < m.MinTimestamp {
		m.MinTimestamp = p.Timestamp
	}
	if p.Timestamp > m.MaxTimestamp {
		m.MaxTimestamp = p.Timestamp
	}

	if p.Value < m.MinValue {
		m.MinValue = p.Value
	}
	if p.Value > m.MaxValue {
		m.MaxValue = p.Value
	}
}

func (m *Metadata) Serialize() []byte {
	allBytes := make([]byte, 0)

	writeUint64 := func(val uint64) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, val)
		allBytes = append(allBytes, b...)
	}

	writeFloat64 := func(val float64) {
		writeUint64(math.Float64bits(val))
	}

	// writing
	writeUint64(m.MinTimestamp)
	writeUint64(m.MaxTimestamp)

	writeFloat64(m.MinValue)
	writeFloat64(m.MaxValue)

	return allBytes
}

func DeserializeMetadata(data []byte) (*Metadata, error) {
	m := &Metadata{}

	var offset int

	readUint64 := func() (uint64, error) {
		if offset+8 > len(data) {
			return 0, errors.New("unexpected EOF while reading uint64")
		}
		val := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		return val, nil
	}

	readFloat64 := func() (float64, error) {
		bits, err := readUint64()
		if err != nil {
			return 0, err
		}
		return math.Float64frombits(bits), nil
	}

	// reading
	var err error
	if m.MinTimestamp, err = readUint64(); err != nil {
		return nil, err
	}
	if m.MaxTimestamp, err = readUint64(); err != nil {
		return nil, err
	}
	if m.MinValue, err = readFloat64(); err != nil {
		return nil, err
	}
	if m.MaxValue, err = readFloat64(); err != nil {
		return nil, err
	}

	return m, nil
}
