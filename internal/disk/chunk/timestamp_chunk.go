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

func (tsc *TimestampChunk) Add(pm *page.Manager, timestamp uint64) error {
	cd := tsc.ActivePage.TimestampCompressor.CompressNext(
		timestamp, tsc.ActivePage.Metadata.Count)

	tse := entry.NewTimestampEntry(timestamp, cd)

	if tse.Size() > tsc.ActivePage.Padding {
		err := pm.WritePage(tsc.ActivePage, tsc.FilePath, int64(tsc.CurrentOffset))
		if err != nil {
			return err
		}
		tsc.CurrentOffset += pm.Config.PageSize

		tsc.ActivePage = page.NewTimestampPage(pm.Config.PageSize)
		tse.CompressedData = tsc.ActivePage.TimestampCompressor.CompressNext(
			timestamp, tsc.ActivePage.Metadata.Count)
	}

	tsc.ActivePage.Add(tse)
	return nil
}

func (tsc *TimestampChunk) Save(pm *page.Manager) (uint64, error) {
	return tsc.CurrentOffset, pm.WritePage(tsc.ActivePage, tsc.FilePath, int64(tsc.CurrentOffset))
}

func (tsc *TimestampChunk) Load(pm *page.Manager) error {
	timestampPageBytes, err := pm.ReadPage(tsc.FilePath, int64(tsc.CurrentOffset))
	timestampPage, err := page.DeserializeTimestampPage(timestampPageBytes)
	if err != nil {
		return err
	}

	tsc.ActivePage = timestampPage.(*page.TimestampPage)
	return nil
}
