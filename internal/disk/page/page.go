package page

type Page struct {
	Metadata *Metadata
	Entries  []*StringEntry
	Padding  uint64
}

func NewPage(pageSize uint64) *Page {
	return &Page{
		Metadata: NewMetadata(),
		Entries:  make([]*StringEntry, 0),
		Padding:  pageSize - 24, // 24 for size of page metadata
	}
}

func (p *Page) AddEntry(entry *StringEntry) {
	p.Entries = append(p.Entries, entry)
	p.Padding -= entry.Size()

	if p.Metadata.MinValue > entry.Value {
		p.Metadata.MinValue = entry.Value
	}
	if p.Metadata.MaxValue < entry.Value {
		p.Metadata.MaxValue = entry.Value
	}
	p.Metadata.Count++
}

func (p *Page) Serialize() []byte {
	allBytes := make([]byte, 0)

	allBytes = append(allBytes, p.Metadata.Serialize()...)

	for _, entry := range p.Entries {
		allBytes = append(allBytes, entry.Serialize()...)
	}

	return allBytes
}
