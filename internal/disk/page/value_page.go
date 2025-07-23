package page

import (
	"errors"
	"math"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/entry"
)

type ValuePage struct {
	Metadata        *Metadata
	Entries         []entry.Entry
	ValueCompressor *entry.ValueCompressor
	Padding         uint64
	pageSize        uint64
}

func NewValuePage(pageSize uint64) *ValuePage {
	return &ValuePage{
		Metadata:        NewMetadata(),
		Entries:         make([]entry.Entry, 0),
		ValueCompressor: entry.NewValueCompressor(),
		Padding:         (pageSize - MetadataSize) * 8, // x8 since value page is working with bits
		pageSize:        pageSize,
	}
}

func (p *ValuePage) Add(e entry.Entry) {
	ve, ok := e.(*entry.ValueEntry)
	if !ok {
		return
	}
	p.Metadata.UpdateMinMaxValue(math.Float64bits(ve.Value))
	p.Metadata.Count++
	p.Entries = append(p.Entries, ve)
	p.Padding -= ve.Size()
}

func (p *ValuePage) Serialize() []byte {
	allBytes := make([]byte, 0)
	allBytes = append(allBytes, p.Metadata.Serialize()...)

	w := internal.NewBitWriter(p.pageSize - MetadataSize)

	for _, e := range p.Entries {
		ve, _ := e.(*entry.ValueEntry)
		if ve.CompressedData.Compressed == false {
			w.WriteBits(uint64(3), 2)
		}
		w.WriteBits(ve.CompressedData.Value, ve.CompressedData.ValueSize)
	}

	for i := uint64(0); i < p.Padding; i++ { // write remaining padding bits
		w.WriteBit(0)
	}

	allBytes = append(allBytes, w.Bytes()...)

	return allBytes
}

func DeserializeValuePage(bytes []byte) (Page, error) {
	pageSize := uint64(len(bytes))
	p := NewValuePage(pageSize)

	p.Metadata = DeserializeMetadata(bytes)
	if p.Metadata == nil {
		return nil, errors.New("[ERROR]: invalid metadata bytes")
	}

	vr := entry.NewValueReconstructor(bytes[MetadataSize:])

	for i := uint64(0); i < p.Metadata.Count; i++ {
		ve := vr.ReconstructNext()
		p.Entries = append(p.Entries, ve)
		p.Padding -= ve.Size()
	}

	p.ValueCompressor.Update(vr.LastValue(), vr.LastLeading(), vr.LastTrailing())

	return p, nil
}
