package page

import "time-series-engine/internal/disk/entry"

type ValuePage struct {
	ValueMetadata *ValueMetadata
	Entries       []*entry.ValueEntry
	Padding       uint64
}

func NewValuePage(pageSize uint64) *ValuePage {
	return &ValuePage{
		ValueMetadata: NewValueMetadata(),
		Entries:       make([]*entry.ValueEntry, 0),
		Padding:       pageSize - 24, // 24 for size of page metadata
	}
}

func (p *ValuePage) AddEntry(e entry.Entry) {
	ve, ok := e.(*entry.ValueEntry)
	if !ok {
		return
	}

	p.Entries = append(p.Entries, ve)
	p.Padding -= e.Size()

	if p.ValueMetadata.MinValue > ve.Value {
		p.ValueMetadata.MinValue = ve.Value
	}
	if p.ValueMetadata.MaxValue < ve.Value {
		p.ValueMetadata.MaxValue = ve.Value
	}
	p.ValueMetadata.Count++
}

func (p *ValuePage) Serialize() []byte {
	allBytes := make([]byte, 0)

	allBytes = append(allBytes, p.ValueMetadata.Serialize()...)

	for _, e := range p.Entries {
		allBytes = append(allBytes, e.Serialize()...)
	}

	return allBytes
}
