package page

import (
	"io"
	"time-series-engine/internal/disk/entry"
)

const CRC = 4

type WALPage struct {
	Entries     []*entry.WALEntry
	paddingSize uint64
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

func NewWALPage(pageSize uint64) *WALPage {
	return &WALPage{
		Entries:     make([]*entry.WALEntry, 0),
		paddingSize: pageSize,
	}
}

func (p *WALPage) PaddingSize() uint64 {
	return p.paddingSize
}
