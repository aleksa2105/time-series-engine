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

func (vc *ValueChunk) Add(pm *page.Manager, value float64) {
	ve := entry.NewValueEntry(value)
	cd := vc.ActivePage.ValueCompressor.CompressNextEntry(ve, vc.ActivePage.Metadata.Count)

	// if there is no space, we need to calculate compressed entry again for empty page
	if uint64(cd.ValueSize) > vc.ActivePage.Padding {
		pm.Write(vc.ActivePage)
		vc.ActivePage = page.NewValuePage(pm.Config.PageSize)
		cd = vc.ActivePage.ValueCompressor.CompressNextEntry(ve, vc.ActivePage.Metadata.Count)
	}

	ve.CompressedData = cd

	vc.ActivePage.Add(ve)
}
