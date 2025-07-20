package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

type ValueChunk struct {
	ActivePage    *page.ValuePage
	FilePath      string
	CurrentOffset uint64
}

func NewValueChunk(pageSize uint64, filePath string) *ValueChunk {
	return &ValueChunk{
		ActivePage:    page.NewValuePage(pageSize),
		FilePath:      filePath,
		CurrentOffset: 0,
	}
}

func (sc *ValueChunk) Add(
	pm *page.Manager, value float64) {
	tse := &entry.ValueEntry{
		Value: value,
	}

	if tse.Size() > sc.ActivePage.Padding {
		pm.WritePage(sc.ActivePage, sc.FilePath, int64(sc.CurrentOffset))
		sc.CurrentOffset += pm.Config.PageSize

		sc.ActivePage = page.NewValuePage(pm.Config.PageSize)
	}

	sc.ActivePage.AddEntry(tse)
}

func (sc *ValueChunk) Save(pm *page.Manager) {
	pm.WritePage(sc.ActivePage, sc.FilePath, int64(sc.CurrentOffset))
}
