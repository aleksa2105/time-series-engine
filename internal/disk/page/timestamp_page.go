package page

import (
	"errors"
	"time-series-engine/internal/disk/entry"
)

type TimestampPage struct {
	Metadata            *Metadata
	Entries             []entry.Entry
	TimestampCompressor *entry.TimestampCompressor
	Padding             uint64
	pageSize            uint64
}

func NewTimestampPage(pageSize uint64) *TimestampPage {
	return &TimestampPage{
		Metadata:            NewMetadata(),
		Entries:             make([]entry.Entry, 0),
		TimestampCompressor: entry.NewTimestampCompressor(),
		Padding:             pageSize - MetadataSize,
		pageSize:            pageSize,
	}
}

func (p *TimestampPage) Add(e entry.Entry) {
	tse, ok := e.(*entry.TimestampEntry)
	if !ok {
		return
	}

	p.Metadata.UpdateMinMaxValue(tse.Value)
	p.Metadata.Count++
	p.Entries = append(p.Entries, tse)
	p.Padding -= tse.Size()
}

func (p *TimestampPage) Serialize() []byte {
	allBytes := make([]byte, 0)
	allBytes = append(allBytes, p.Metadata.Serialize()...)

	for _, e := range p.Entries {
		tse, _ := e.(*entry.TimestampEntry)
		allBytes = append(allBytes, tse.Serialize()...)
	}

	paddingBytes := make([]byte, p.Padding)
	allBytes = append(allBytes, paddingBytes...)

	return allBytes
}

func DeserializeTimestampPage(bytes []byte) (Page, error) {
	pageSize := uint64(len(bytes))
	p := NewTimestampPage(pageSize)

	p.Metadata = DeserializeMetadata(bytes)
	if p.Metadata == nil {
		return nil, errors.New("[ERROR]: invalid timestamp page")
	}

	tsr := entry.NewTimestampReconstructor(bytes[MetadataSize:])

	for i := uint64(0); i < p.Metadata.Count; i++ {
		tse := tsr.ReconstructNext()
		p.Entries = append(p.Entries, tse)
		p.Padding -= tse.Size()
	}

	p.TimestampCompressor.Update(tsr.LastValue())

	return p, nil
}
