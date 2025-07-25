package chunk

import (
	"os"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/page/page_manager"
)

type DeleteChunk struct {
	ActivePage    *page.DeletePage
	FilePath      string
	CurrentOffset uint64
}

func NewDeleteChunk(pageSize uint64, filePath string) *DeleteChunk {
	return &DeleteChunk{
		ActivePage:    page.NewDeletePage(pageSize),
		FilePath:      filePath,
		CurrentOffset: 0,
	}
}

func (dc *DeleteChunk) Add(pm *page_manager.Manager, deleted bool) error {
	de := entry.NewDeleteEntry(deleted)

	if de.Size() > dc.ActivePage.Padding {
		err := pm.WritePage(dc.ActivePage, dc.FilePath, int64(dc.CurrentOffset))
		if err != nil {
			return err
		}
		dc.CurrentOffset += pm.Config.PageSize
		dc.ActivePage = page.NewDeletePage(pm.Config.PageSize)
	}

	dc.ActivePage.Add(de)
	return nil
}

func (dc *DeleteChunk) Save(pm *page_manager.Manager) error {
	return pm.WritePage(dc.ActivePage, dc.FilePath, int64(dc.CurrentOffset))
}

func (dc *DeleteChunk) Load(pm *page_manager.Manager) error {
	fileInfo, err := os.Stat(dc.FilePath)
	if err != nil {
		return err
	}
	deletePageBytes, err := pm.ReadPage(dc.FilePath, fileInfo.Size()-int64(pm.Config.PageSize))
	p, err := page.DeserializeDeletePage(deletePageBytes)
	if err != nil {
		return err
	}

	dc.ActivePage = p.(*page.DeletePage)
	return nil
}
