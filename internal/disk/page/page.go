package page

import "time-series-engine/internal/disk/entry"

type Page interface {
	Add(entry entry.Entry)
	Serialize() []byte
	EntryCount() uint64
	GetEntries() []entry.Entry
	GetMetadata() *Metadata
}
