package page

import (
	"errors"
	"math"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/entry"
)

type ValuePage struct {
	Metadata        *Metadata
	BitWriter       *internal.BitWriter
	ValueCompressor *entry.ValueCompressor
	Padding         uint64
}

func NewValuePage(pageSize uint64) *ValuePage {
	return &ValuePage{
		Metadata:        NewMetadata(),
		BitWriter:       internal.NewBitWriter(pageSize - MetadataSize),
		ValueCompressor: entry.NewValueCompressor(),
		Padding:         (pageSize - MetadataSize) * 8, // x8 since value page is working with bits
	}
}

func (p *ValuePage) Add(e entry.Entry) {
	ve, ok := e.(*entry.ValueEntry)
	if !ok {
		return
	}
	p.Metadata.UpdateMinMaxValue(math.Float64bits(ve.Value))
	p.Metadata.Count++
	p.BitWriter.WriteBits(ve.CompressedData.Value, ve.CompressedData.ValueSize)
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

func DeserializeValuePage(bytes []byte) (*Metadata, []entry.Entry, error) {
	md := DeserializeMetadata(bytes)
	if md == nil {
		return nil, nil, errors.New("[ERROR]: invalid metadata bytes")
	}

	entries := make([]entry.Entry, 0, md.Count)
	r := internal.NewBitReader(bytes[MetadataSize:])
	vd := entry.NewValueDecompressor(r)

	for i := uint64(0); i < md.Count; i++ {
		entries = append(entries, vd.DecompressNextValue(i))
	}

	return md, entries, nil
}
