package row_group

import (
	"path/filepath"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/chunk"
	"time-series-engine/internal/disk/page"
)

type RowGroup struct {
	PageManager    *page.Manager
	Metadata       *Metadata
	TimestampChunk *chunk.TimestampChunk
	ValueChunk     *chunk.ValueChunk
	PointsNumber   uint64
	DirectoryPath  string
}

func NewRowGroup(ts *internal.TimeSeries, pm *page.Manager, path string) (*RowGroup, error) {
	files := make([]*string, 0, 3) // metadata + timestamp + value + tags
	filePathMetadata := filepath.Join(path, "metadata.db")
	filePathTimestamp := filepath.Join(path, "timestamp.db")
	filePathValue := filepath.Join(path, "value.db")
	files = append(files, &filePathMetadata, &filePathTimestamp, &filePathValue)

	err := createFiles(pm, files)
	if err != nil {
		return nil, err
	}

	return &RowGroup{
		PageManager:    pm,
		Metadata:       NewMetadata(),
		TimestampChunk: chunk.NewTimestampChunk(pm.Config.PageSize, filePathTimestamp),
		ValueChunk:     chunk.NewValueChunk(pm.Config.PageSize, filePathValue),
		PointsNumber:   0,
		DirectoryPath:  path,
	}, nil
}

func (rg *RowGroup) AddPoint(p *internal.Point) {
	rg.Metadata.Update(p)

	rg.TimestampChunk.Add(rg.PageManager, p.Timestamp)
	rg.ValueChunk.Add(rg.PageManager, p.Value)

	rg.PointsNumber++
}

func (rg *RowGroup) Save(pm *page.Manager) {
	filePathMetadata := filepath.Join(rg.DirectoryPath, "metadata.db")
	pm.WriteStructure(rg.Metadata.Serialize(), filePathMetadata, 0)

	rg.TimestampChunk.Save(pm)
	rg.ValueChunk.Save(pm)
}

func createFiles(pm *page.Manager, files []*string) error {
	for _, file := range files {
		err := pm.CreateFile(*file)
		if err != nil {
			return err
		}
	}

	return nil
}
