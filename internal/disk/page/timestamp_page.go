package page

import "time-series-engine/internal/disk/entry"

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

func (p *TimestampPage) AddEntry(e entry.Entry) {
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

func DeserializeTimestampPage(bytes []byte) []*entry.TimestampEntry {
	metadata := DeserializeMetadata(bytes)

	var (
		offset  = MetadataSize
		entries = make([]*entry.TimestampEntry, 0, metadata.Count)
	)

	firstEntry, n := entry.DeserializeTimestampEntry(bytes[offset:])
	if n <= 0 {
		return nil
	}
	offset += n
	entries = append(entries, firstEntry)

	lastValue := firstEntry.Value
	// deserialize remaining delta encoded entries
	for i := uint64(1); i < metadata.Count; i++ {
		e, n := entry.DeserializeTimestampEntry(bytes[offset:])
		if n <= 0 {
			return nil
		}
		offset += n
		e.Value += lastValue
		lastValue = e.Value
		entries = append(entries, e)
	}

	return entries
}
