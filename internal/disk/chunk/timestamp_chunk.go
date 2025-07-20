package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

type TimestampChunk struct {
	ActivePage    *page.TimestampPage
	FilePath      string
	CurrentOffset uint64
}

func NewTimestampChunk(pageSize uint64, filePath string) *TimestampChunk {
	return &TimestampChunk{
		ActivePage:    page.NewTimestampPage(pageSize),
		FilePath:      filePath,
		CurrentOffset: 0,
	}
}

func (tsc *TimestampChunk) Add(pm *page.Manager, timestamp uint64) {
	tse := &entry.TimestampEntry{
		Value: timestamp,
	}

	if len(tsc.ActivePage.Entries) != 0 {
		tse.Value -= tsc.ActivePage.Entries[len(tsc.ActivePage.Entries)-1].Value
	}

	if tse.Size() > tsc.ActivePage.Padding {
		pm.WritePage(tsc.ActivePage, tsc.FilePath, int64(tsc.CurrentOffset))
		tsc.CurrentOffset += pm.Config.PageSize

		tsc.ActivePage = page.NewTimestampPage(pm.Config.PageSize)
	}

	tsc.ActivePage.AddEntry(tse)
}

func (tsc *TimestampChunk) Save(pm *page.Manager) {
	pm.WritePage(tsc.ActivePage, tsc.FilePath, int64(tsc.CurrentOffset))
}
