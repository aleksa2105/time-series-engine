package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/page/page_manager"
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

func (vc *ValueChunk) Add(pm *page_manager.Manager, value float64) error {
	cd := vc.ActivePage.ValueCompressor.CompressNext(value, vc.ActivePage.Metadata.Count)
	ve := entry.NewValueEntry(value, cd)

	// if there is no space, we need to calculate compressed entry again for empty page
	if ve.Size() > vc.ActivePage.Padding {
		err := pm.WritePage(vc.ActivePage, vc.FilePath, int64(vc.CurrentOffset))
		if err != nil {
			return err
		}

		vc.CurrentOffset += pm.Config.PageSize

		vc.ActivePage = page.NewValuePage(pm.Config.PageSize)
		ve.CompressedData = vc.ActivePage.ValueCompressor.CompressNext(value, vc.ActivePage.Metadata.Count)
	}

	vc.ActivePage.Add(ve)
	return nil
}

func (vc *ValueChunk) Save(pm *page_manager.Manager) error {
	return pm.WritePage(vc.ActivePage, vc.FilePath, int64(vc.CurrentOffset))
}

func (vc *ValueChunk) Load(pm *page_manager.Manager) error {
	valuePageBytes, err := pm.ReadPage(vc.FilePath, int64(vc.CurrentOffset))
	valuePage, err := page.DeserializeValuePage(valuePageBytes)
	if err != nil {
		return err
	}

	vc.ActivePage = valuePage.(*page.ValuePage)
	return nil
}
