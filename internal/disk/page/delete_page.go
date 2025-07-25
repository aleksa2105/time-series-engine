package page

import (
	"errors"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/entry"
)

type DeletePage struct {
	Metadata *Metadata
	Entries  []entry.Entry
	Padding  uint64
	pageSize uint64
}

func NewDeletePage(pageSize uint64) *DeletePage {
	return &DeletePage{
		Metadata: NewMetadata(),
		Entries:  make([]entry.Entry, 0),
		Padding:  (pageSize - MetadataSize) * 8, // x8 since delete page is working with bits
		pageSize: pageSize,
	}
}

func (p *DeletePage) Add(e entry.Entry) {
	de, ok := e.(*entry.DeleteEntry)
	if !ok {
		return
	}

	p.Metadata.Count++
	p.Entries = append(p.Entries, de)
	p.Padding -= de.Size()
}

func (p *DeletePage) Serialize() []byte {
	allBytes := make([]byte, 0)
	allBytes = append(allBytes, p.Metadata.Serialize()...)

	w := internal.NewBitWriter(p.pageSize - MetadataSize)

	for _, e := range p.Entries {
		de, _ := e.(*entry.DeleteEntry)
		if de.Deleted == true {
			w.WriteBit(entry.DeletedBit)
		} else {
			w.WriteBit(entry.ActiveBit)
		}
	}

	for i := uint64(0); i < p.Padding; i++ { // write remaining padding bits
		w.WriteBit(0)
	}

	allBytes = append(allBytes, w.Bytes()...)
	return allBytes
}

func DeserializeDeletePage(bytes []byte) (Page, error) {
	pageSize := uint64(len(bytes))
	p := NewDeletePage(pageSize)

	p.Metadata = DeserializeMetadata(bytes)
	if p.Metadata == nil {
		return nil, errors.New("[ERROR]: invalid metadata bytes")
	}

	r := internal.NewBitReader(bytes[MetadataSize:])

	for i := uint64(0); i < p.Metadata.Count; i++ {
		bit, _ := r.ReadBit()
		e := &entry.DeleteEntry{}
		if bit == entry.DeletedBit {
			e.Deleted = true
		} else {
			e.Deleted = false
		}
		p.Entries = append(p.Entries, e)
		p.Padding -= e.Size()
	}

	return p, nil
}

func (p *DeletePage) EntryCount() uint64 {
	return p.Metadata.Count
}

func (p *DeletePage) GetEntries() []entry.Entry {
	return p.Entries
}

func (p *DeletePage) GetMetadata() *Metadata {
	return p.Metadata
}
