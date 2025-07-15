package page

import "time-series-engine/internal/disk/entry"

type TimestampPage struct {
	Metadata *Metadata
	Entries  []*entry.TimestampEntry
	Padding  uint64
}

func NewTimestampPage(pageSize uint64) *TimestampPage {
	return &TimestampPage{
		Metadata: NewMetadata(),
		Entries:  make([]*entry.TimestampEntry, 0),
		Padding:  pageSize - 24, // 24 for size of page metadata
	}
}

func (p *TimestampPage) AddEntry(e entry.Entry) {
	tse, ok := e.(*entry.TimestampEntry)
	if !ok {
		return
	}

	p.Entries = append(p.Entries, tse)
	p.Padding -= e.Size()

	if p.Metadata.MinValue > tse.Value {
		p.Metadata.MinValue = tse.Value
	}
	if p.Metadata.MaxValue < tse.Value {
		p.Metadata.MaxValue = tse.Value
	}
	p.Metadata.Count++
}

func (p *TimestampPage) Serialize() []byte {
	allBytes := make([]byte, 0)

	allBytes = append(allBytes, p.Metadata.Serialize()...)

	for _, e := range p.Entries {
		allBytes = append(allBytes, e.Serialize()...)
	}

	return allBytes
}
