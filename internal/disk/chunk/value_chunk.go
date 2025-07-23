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
	cd := vc.ActivePage.ValueCompressor.CompressNextValue(value, vc.ActivePage.Metadata.Count)

	// if there is no space, we need to calculate compressed entry again for empty page
	if uint64(cd.ValueSize) > vc.ActivePage.Padding {
		pm.Write(vc.ActivePage)
		vc.ActivePage = page.NewValuePage(pm.Config.PageSize)
		cd = vc.ActivePage.ValueCompressor.CompressNextValue(value, vc.ActivePage.Metadata.Count)
	}

	ve := entry.NewValueEntry(value)
	ve.CompressedData = cd

	vc.ActivePage.Add(ve)
}

func (sc *ValueChunk) Save(pm *page.Manager) {
	pm.WritePage(sc.ActivePage, sc.FilePath, int64(sc.CurrentOffset))
}
