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
	DirectoryPath  string
}

func NewRowGroup(pm *page.Manager, path string, rgIndex uint64) (*RowGroup, error) {
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
		Metadata:       NewMetadata(rgIndex),
		TimestampChunk: chunk.NewTimestampChunk(pm.Config.PageSize, filePathTimestamp),
		ValueChunk:     chunk.NewValueChunk(pm.Config.PageSize, filePathValue),
		DirectoryPath:  path,
	}, nil
}

func (rg *RowGroup) AddPoint(p *internal.Point) error {
	rg.Metadata.Update(p)

	err := rg.TimestampChunk.Add(rg.PageManager, p.Timestamp)
	if err != nil {
		return err
	}
	err = rg.ValueChunk.Add(rg.PageManager, p.Value)
	if err != nil {
		return err
	}
	return nil
}

func (rg *RowGroup) Save(pm *page.Manager) error {
	filePathMetadata := filepath.Join(rg.DirectoryPath, "metadata.db")
	err := pm.WriteStructure(rg.Metadata.Serialize(), filePathMetadata, 0)
	if err != nil {
		return err
	}

	err = rg.TimestampChunk.Save(pm)
	if err != nil {
		return err
	}

	err = rg.ValueChunk.Save(pm)
	if err != nil {
		return err
	}
	return nil
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

func LoadRowGroup(pm *page.Manager, path string) (*RowGroup, error) {
	metaPath := filepath.Join(path, "metadata.db")
	timestampPath := filepath.Join(path, "timestamp.db")
	valuePath := filepath.Join(path, "value.db")

	rg := &RowGroup{
		PageManager:    pm,
		DirectoryPath:  path,
		TimestampChunk: nil,
		ValueChunk:     nil,
		Metadata:       nil,
	}

	data, err := pm.ReadStructure(metaPath, 0)
	if err != nil {
		return nil, err
	}

	meta, err := DeserializeMetadata(data)
	if err != nil {
		return nil, err
	}

	rg.Metadata = meta

	timestampChunk := &chunk.TimestampChunk{
		ActivePage:    nil,
		FilePath:      timestampPath,
		CurrentOffset: meta.TimestampOffset,
	}
	err = timestampChunk.Load(pm)
	if err != nil {
		return nil, err
	}

	valueChunk := &chunk.ValueChunk{
		ActivePage:    nil,
		FilePath:      valuePath,
		CurrentOffset: meta.ValueOffset,
	}
	err = valueChunk.Load(pm)
	if err != nil {
		return nil, err
	}

	rg.TimestampChunk = timestampChunk
	rg.ValueChunk = valueChunk

	return rg, nil
}
