package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

type ValueChunk struct {
	ActivePage *page.ValuePage
}

func NewValueChunk(pageSize uint64) *ValueChunk {
	return &ValueChunk{
		ActivePage: page.NewValuePage(pageSize),
	}
}

func (sc *ValueChunk) Add(
	pm *page.Manager, value float64) {
	tse := &entry.ValueEntry{
		Value: value,
	}

	if tse.Size() > sc.ActivePage.Padding {
		pm.Write(sc.ActivePage)
		sc.ActivePage = page.NewValuePage(pm.Config.PageSize)
	}

	sc.ActivePage.AddEntry(tse)
}
