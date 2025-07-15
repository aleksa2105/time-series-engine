package page

import "time-series-engine/internal/disk/entry"

type StringPage struct {
	Metadata *Metadata
	Entries  []*entry.StringEntry
	Padding  uint64
}

func NewStringPage(pageSize uint64) *StringPage {
	return &StringPage{
		Metadata: NewMetadata(),
		Entries:  make([]*entry.StringEntry, 0),
		Padding:  pageSize - 24, // 24 for size of page metadata
	}
}

func (p *StringPage) AddEntry(e entry.Entry) {
	se, ok := e.(*entry.StringEntry)
	if !ok {
		return
	}

	p.Entries = append(p.Entries, se)
	p.Padding -= se.Size()

	if p.Metadata.MinValue > se.Value {
		p.Metadata.MinValue = se.Value
	}
	if p.Metadata.MaxValue < se.Value {
		p.Metadata.MaxValue = se.Value
	}
	p.Metadata.Count++
}

func (p *StringPage) Serialize() []byte {
	allBytes := make([]byte, 0)

	allBytes = append(allBytes, p.Metadata.Serialize()...)

	for _, e := range p.Entries {
		allBytes = append(allBytes, e.Serialize()...)
	}

	return allBytes
}
