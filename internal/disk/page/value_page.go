package page

import (
	"time-series-engine/internal/disk/entry"
)

type ValuePage struct {
	Metadata        *Metadata
	BitWriter       *BitWriter
	ValueCompressor *entry.ValueCompressor
	Padding         uint64
}

func NewValuePage(pageSize uint64) *ValuePage {
	return &ValuePage{
		Metadata:        NewMetadata(),
		BitWriter:       NewBitWriter(pageSize - MetadataSize),
		ValueCompressor: entry.NewValueCompressor(),
		Padding:         (pageSize - MetadataSize) * 8, // x8 since value page is working with bits
	}
}

func (p *ValuePage) AddEntry(e entry.Entry) {
	ve, ok := e.(*entry.ValueEntry)
	if !ok {
		return
	}
	p.Metadata.UpdateMinMaxValue(ve.Value)
	p.Metadata.Count++
	p.BitWriter.WriteBits(ve.CompressedData.Value, ve.CompressedData.ValueSize)
	p.ValueCompressor.Update(ve.Value, ve.CompressedData.Leading, ve.CompressedData.Trailing)
	p.Padding -= ve.Size()
}

func (p *ValuePage) Serialize() []byte {
	allBytes := make([]byte, 0)
	allBytes = append(allBytes, p.Metadata.Serialize()...)

	for i := uint64(0); i < p.Padding; i++ { // write remaining padding bits
		p.BitWriter.WriteBit(0)
	}

	allBytes = append(allBytes, p.BitWriter.Bytes()...)

	return allBytes
}
