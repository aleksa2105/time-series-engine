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

func (vc *ValueChunk) Add(pm *page.Manager, value float64) {
	cd := vc.ActivePage.ValueCompressor.CompressNext(value, vc.ActivePage.Metadata.Count)
	ve := entry.NewValueEntry(value, cd)

	// if there is no space, we need to calculate compressed entry again for empty page
	if ve.Size() > vc.ActivePage.Padding {
		pm.WritePage(vc.ActivePage)
		vc.ActivePage = page.NewValuePage(pm.Config.PageSize)
		ve.CompressedData = vc.ActivePage.ValueCompressor.CompressNext(value, vc.ActivePage.Metadata.Count)
	}

	vc.ActivePage.Add(ve)
}

func (vc *ValueChunk) Save(pm *page.Manager) {
	pm.WritePage(vc.ActivePage, vc.FilePath, int64(vc.CurrentOffset))
}
