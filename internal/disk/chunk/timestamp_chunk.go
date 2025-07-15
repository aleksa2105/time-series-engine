package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

type TimestampChunk struct {
	ActivePage *page.TimestampPage
}

func NewTimestampChunk(pageSize uint64) *TimestampChunk {
	return &TimestampChunk{
		ActivePage: page.NewTimestampPage(pageSize),
	}
}

func (sc *TimestampChunk) Add(
	pm *page.Manager, timestamp uint64) {

	tse := &entry.TimestampEntry{
		Value: timestamp,
	}

	if len(sc.ActivePage.Entries) != 0 {
		tse.Value -= sc.ActivePage.Entries[len(sc.ActivePage.Entries)-1].Value
	}

	if tse.Size() > sc.ActivePage.Padding {
		pm.Write(sc.ActivePage)
		sc.ActivePage = page.NewTimestampPage(pm.Config.PageSize)
	}

	sc.ActivePage.AddEntry(tse)
}
