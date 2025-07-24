package page

import (
	"io"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/entry"
)

const CRC = 4

type PageKey struct {
	Filename string
	Offset   int64
}

type WALPage struct {
	Entries     []*entry.WALEntry
	paddingSize uint64
	key         PageKey
}

func (p *WALPage) Add(e entry.Entry) {
	p.Entries = append(p.Entries, e.(*entry.WALEntry))
	p.paddingSize -= e.Size()
}

func (p *WALPage) Serialize() []byte {
	allDataBytes := make([]byte, 0)

	for _, e := range p.Entries {
		allDataBytes = append(allDataBytes, e.Serialize()...)
	}

	paddingBytes := make([]byte, p.paddingSize)
	allDataBytes = append(allDataBytes, paddingBytes...)

	return allDataBytes
}

func DeserializeWALPage(data []byte) (*WALPage, error) {
	var offset uint64 = 0
	p := &WALPage{}
	p.Entries = make([]*entry.WALEntry, 0)

	for offset+CRC < uint64(len(data)) {

		e := &entry.WALEntry{}

		err := e.Deserialize(data[offset:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		entrySize := e.Size()
		offset += entrySize

		p.Entries = append(p.Entries, e)
	}

	p.paddingSize = uint64(len(data)) - offset
	return p, nil
}

func (p *WALPage) Put(timeSeries *internal.TimeSeries, point *internal.Point) {
	e := entry.NewWALEntry(timeSeries, point)
	p.Add(e)
}

func NewWALPage(pageSize uint64) *WALPage {
	return &WALPage{
		Entries:     make([]*entry.WALEntry, 0),
		paddingSize: pageSize,
		key:         PageKey{},
	}
}

func (p *WALPage) SetKey(filename string, offset int64) {
	p.key.Filename = filename
	p.key.Offset = offset
}

func (p *WALPage) PaddingSize() uint64 {
	return p.paddingSize
}
