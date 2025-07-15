package page

import "time-series-engine/internal/disk/entry"

type Page interface {
	AddEntry(entry entry.Entry)
	Serialize() []byte
}
