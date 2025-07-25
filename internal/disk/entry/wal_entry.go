package entry

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"time-series-engine/internal"
)

const CRC = 4
const TOMBSTONE = 1
const MEASUREMENT_NAME_SIZE = 8
const NUMBER_OF_TAGS = 8
const TIMESTAMP = 8
const VALUE = 8

type WALEntry struct {
	CRC                 uint32
	Delete              bool
	MeasurementNameSize uint64
	MeasurementName     string
	NumberOfTags        uint64
	Tags                internal.Tags
	MinTimestamp        uint64
	MaxTimestamp        uint64
	Value               float64
}

func (e *WALEntry) GetValue() uint64 {
	return 0
}

func NewWALDeleteEntry(timeSeries *internal.TimeSeries, minTimestamp, maxTimestamp uint64) *WALEntry {
	mn := timeSeries.MeasurementName
	t := timeSeries.Tags
	we := WALEntry{
		CRC:                 0,
		Delete:              true,
		MeasurementNameSize: uint64(len(mn)),
		MeasurementName:     mn,
		NumberOfTags:        uint64(t.Len()),
		Tags:                t,
		MinTimestamp:        minTimestamp,
		MaxTimestamp:        maxTimestamp,
		Value:               0.0,
	}
	we.calculateCRC()
	return &we
}

func NewWALPutEntry(timeSeries *internal.TimeSeries, point *internal.Point) *WALEntry {
	mn := timeSeries.MeasurementName
	t := timeSeries.Tags
	we := WALEntry{
		CRC:                 0,
		Delete:              false,
		MeasurementNameSize: uint64(len(mn)),
		MeasurementName:     mn,
		NumberOfTags:        uint64(t.Len()),
		Tags:                t,
		MinTimestamp:        point.Timestamp,
		MaxTimestamp:        point.Timestamp,
		Value:               point.Value,
	}
	we.calculateCRC()
	return &we
}

func (e *WALEntry) Deserialize(data []byte) error {
	offset := 0

	e.CRC = binary.BigEndian.Uint32(data[offset:])
	if e.CRC == 0 {
		return io.EOF
	}
	offset += 4

	e.Delete = data[offset] == 1
	offset++

	e.MeasurementNameSize = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	e.MeasurementName = string(data[offset : offset+int(e.MeasurementNameSize)])
	offset += int(e.MeasurementNameSize)

	e.NumberOfTags = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	var tagsSize int
	e.Tags, tagsSize = internal.DeserializeTags(data[offset:], e.NumberOfTags)
	offset += tagsSize

	e.MinTimestamp = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	e.MaxTimestamp = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	e.Value = math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))

	return nil
}

func (e *WALEntry) Serialize() []byte {
	e.calculateCRC()

	buffer := make([]byte, 0)

	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, e.CRC)
	buffer = append(buffer, crcBytes...)

	if e.Delete {
		buffer = append(buffer, 1)
	} else {
		buffer = append(buffer, 0)
	}

	mnSizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(mnSizeBytes, e.MeasurementNameSize)
	buffer = append(buffer, mnSizeBytes...)

	buffer = append(buffer, []byte(e.MeasurementName)...)

	numTagsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numTagsBytes, e.NumberOfTags)
	buffer = append(buffer, numTagsBytes...)

	buffer = append(buffer, e.Tags.Serialize()...)

	minTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minTimestampBytes, e.MinTimestamp)
	buffer = append(buffer, minTimestampBytes...)

	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, e.MaxTimestamp)
	buffer = append(buffer, maxTimestampBytes...)

	valueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBytes, math.Float64bits(e.Value))
	buffer = append(buffer, valueBytes...)

	return buffer
}

func (e *WALEntry) Size() uint64 {
	size := uint64(0)

	size += CRC
	size += TOMBSTONE

	size += MEASUREMENT_NAME_SIZE
	size += e.MeasurementNameSize

	size += NUMBER_OF_TAGS
	size += e.Tags.Size()

	size += 2 * TIMESTAMP
	size += VALUE

	return size
}

func (e *WALEntry) calculateCRC() {
	allDataBytes := make([]byte, 0)

	var tombstoneByte byte = 0
	if e.Delete {
		tombstoneByte = 1
	}
	allDataBytes = append(allDataBytes, tombstoneByte)

	mnSizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(mnSizeBytes, e.MeasurementNameSize)
	allDataBytes = append(allDataBytes, mnSizeBytes...)

	allDataBytes = append(allDataBytes, []byte(e.MeasurementName)...)

	numTagsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numTagsBytes, e.NumberOfTags)
	allDataBytes = append(allDataBytes, numTagsBytes...)

	tagBytes := e.Tags.Serialize()
	allDataBytes = append(allDataBytes, tagBytes...)

	minTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minTimestampBytes, e.MinTimestamp)
	allDataBytes = append(allDataBytes, minTimestampBytes...)

	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, e.MaxTimestamp)
	allDataBytes = append(allDataBytes, maxTimestampBytes...)

	valueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBytes, math.Float64bits(e.Value))
	allDataBytes = append(allDataBytes, valueBytes...)

	e.CRC = CRC32(allDataBytes)
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
