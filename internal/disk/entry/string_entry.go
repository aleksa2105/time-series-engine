package entry

import (
	"encoding/binary"
	"errors"
)

type StringEntry struct {
	Value          uint64
	NumRepetitions uint64
}

func NewStringEntry(value uint64) *StringEntry {
	return &StringEntry{
		Value:          value,
		NumRepetitions: 1,
	}
}

func (e *StringEntry) IncreaseNumRepetitions() {
	e.NumRepetitions++
}

func (e *StringEntry) Serialize() []byte {
	allBytes := make([]byte, 0)

	valueBytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(valueBytes, e.Value)
	allBytes = append(allBytes, valueBytes[:n]...)

	numRepetitionsBytes := make([]byte, binary.MaxVarintLen64)
	n = binary.PutUvarint(numRepetitionsBytes, e.NumRepetitions)
	allBytes = append(allBytes, numRepetitionsBytes[:n]...)

	return allBytes
}

func DeserializeStringEntry(bytes []byte) (*StringEntry, uint64, error) {
	var bytesRead uint64 = 0

	value, n := binary.Uvarint(bytes)
	if n <= 0 {
		return nil, 0, errors.New("invalid entry value bytes")
	}
	bytesRead += uint64(n)

	numReps, n := binary.Uvarint(bytes[n:])
	if n <= 0 {
		return nil, 0, errors.New("invalid entry num repetitions bytes")
	}
	bytesRead += uint64(n)

	return &StringEntry{
		Value:          value,
		NumRepetitions: numReps,
	}, bytesRead, nil
}

func (e *StringEntry) Size() uint64 {
	size := 0

	valueBytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(valueBytes, e.Value)
	size += n

	n = binary.PutUvarint(valueBytes, e.NumRepetitions)
	size += n

	return uint64(size)
}
