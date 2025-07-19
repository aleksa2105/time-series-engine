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

func (tsc *TimestampChunk) Add(pm *page.Manager, timestamp uint64) {
	tse := entry.NewTimestampEntry(timestamp)

	if tse.Size() > tsc.ActivePage.Padding {
		pm.Write(tsc.ActivePage)
		tsc.ActivePage = page.NewTimestampPage(pm.Config.PageSize)
	}

	tsc.ActivePage.AddEntry(tse)
}
