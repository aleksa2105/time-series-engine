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
	tse := entry.NewTimestampEntry(timestamp)

	if tse.Size() > tsc.ActivePage.Padding {
		pm.Write(tsc.ActivePage)
		tsc.ActivePage = page.NewTimestampPage(pm.Config.PageSize)
	}

	tsc.ActivePage.Add(tse)
}

func (tsc *TimestampChunk) Save(pm *page.Manager) {
	pm.WritePage(tsc.ActivePage, tsc.FilePath, int64(tsc.CurrentOffset))
}
