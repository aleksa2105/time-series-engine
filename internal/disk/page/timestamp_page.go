package page

import (
	"errors"
	"time-series-engine/internal/disk/entry"
)

type TimestampPage struct {
	Metadata   *Metadata
	Padding    uint64
	lastValue  uint64
	serialized []byte
}

func NewTimestampPage(pageSize uint64) *TimestampPage {
	return &TimestampPage{
		Metadata:   NewMetadata(),
		Padding:    pageSize - MetadataSize,
		lastValue:  0,
		serialized: make([]byte, 0, pageSize-MetadataSize),
	}
}

func (p *TimestampPage) Add(e entry.Entry) {
	tse, ok := e.(*entry.TimestampEntry)
	if !ok {
		return
	}

	p.Metadata.UpdateMinMaxValue(tse.Value)

	// apply delta
	delta := tse.Value - p.lastValue
	p.lastValue = tse.Value
	tse.Value = delta

	serializedTse := tse.Serialize()
	p.Padding -= uint64(len(serializedTse))
	p.serialized = append(p.serialized, serializedTse...)

	p.Metadata.Count++
}

func (p *TimestampPage) Serialize() []byte {
	allBytes := make([]byte, 0)
	allBytes = append(allBytes, p.Metadata.Serialize()...)

	allBytes = append(allBytes, p.serialized...)

	paddingBytes := make([]byte, p.Padding)
	allBytes = append(allBytes, paddingBytes...)

	return allBytes
}

func DeserializeTimestampPage(bytes []byte) (*Metadata, []entry.Entry, error) {
	md := DeserializeMetadata(bytes)

	var (
		offset  = MetadataSize
		entries = make([]entry.Entry, 0, md.Count)
	)

	firstEntry, n := entry.DeserializeTimestampEntry(bytes[offset:])
	if n <= 0 {
		return nil, nil, errors.New("[ERROR]: invalid first timestamp entry")
	}
	offset += n
	entries = append(entries, firstEntry)

	lastValue := firstEntry.Value
	// deserialize remaining delta encoded entries
	for i := uint64(1); i < md.Count; i++ {
		e, n := entry.DeserializeTimestampEntry(bytes[offset:])
		if n <= 0 {
			return nil, nil, errors.New("[ERROR]: invalid timestamp entry")
		}
		offset += n
		e.Value += lastValue
		lastValue = e.Value
		entries = append(entries, e)
	}

	return md, entries, nil
}
