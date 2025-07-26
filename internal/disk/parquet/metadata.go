package parquet

import (
	"encoding/binary"
	"errors"
	"math"
)

type Metadata struct {
	MinTimestamp   uint64
	MaxTimestamp   uint64
	PointsNumber   uint64
	TimeSeriesHash string
	MinValue       float64
	MaxValue       float64
	SumValue       float64
}

func NewMetadata(timeSeriesHash string) *Metadata {
	return &Metadata{
		MinTimestamp:   math.MaxUint64,
		MaxTimestamp:   0,
		TimeSeriesHash: timeSeriesHash,
		MinValue:       math.MaxFloat64,
		MaxValue:       0,
		SumValue:       0,
	}
}

func (m *Metadata) Serialize() []byte {
	allBytes := make([]byte, 0)

	writeUint64 := func(val uint64) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, val)
		allBytes = append(allBytes, b...)
	}

	writeUint64(m.MinTimestamp)
	writeUint64(m.MaxTimestamp)
	writeUint64(m.PointsNumber)
	writeUint64(math.Float64bits(m.MinValue))
	writeUint64(math.Float64bits(m.MaxValue))
	writeUint64(math.Float64bits(m.SumValue))

	writeUint64(uint64(len(m.TimeSeriesHash)))
	allBytes = append(allBytes, m.TimeSeriesHash...)

	return allBytes
}

func DeserializeParquetMetadata(data []byte) (*Metadata, error) {
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

	// reading
	var err error
	var hashLength uint64
	if m.MinTimestamp, err = readUint64(); err != nil {
		return nil, err
	}
	if m.MaxTimestamp, err = readUint64(); err != nil {
		return nil, err
	}
	if m.PointsNumber, err = readUint64(); err != nil {
		return nil, err
	}
	minBits, err := readUint64()
	if err != nil {
		return nil, err
	}
	m.MinValue = math.Float64frombits(minBits)

	maxBits, err := readUint64()
	if err != nil {
		return nil, err
	}
	m.MaxValue = math.Float64frombits(maxBits)

	sumBits, err := readUint64()
	if err != nil {
		return nil, err
	}
	m.SumValue = math.Float64frombits(sumBits)

	if hashLength, err = readUint64(); err != nil {
		return nil, err
	}
	if offset+int(hashLength) > len(data) {
		return nil, errors.New("unexpected EOF while reading timestamp hash")
	}
	m.TimeSeriesHash = string(data[offset : offset+int(hashLength)])

	return m, nil
}

func (m *Metadata) Update(timestamp uint64, value float64) {
	if timestamp < m.MinTimestamp {
		m.MinTimestamp = timestamp
	}
	m.MaxTimestamp = timestamp
	m.PointsNumber++
	if m.MinValue > value {
		m.MinValue = value
	}
	if m.MaxValue < value {
		m.MaxValue = value
	}
	m.SumValue += value
}
