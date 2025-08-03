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
	DeleteChunk    *chunk.DeleteChunk
	DirectoryPath  string
}

func NewRowGroup(pm *page.Manager, path string, rgIndex uint64) (*RowGroup, error) {
	files := make([]*string, 0, 4) // metadata + timestamp + value + delete
	filePathMetadata := filepath.Join(path, "metadata.db")
	filePathTimestamp := filepath.Join(path, "timestamp.db")
	filePathValue := filepath.Join(path, "value.db")
	filePathDelete := filepath.Join(path, "delete.db")
	files = append(files, &filePathMetadata, &filePathTimestamp, &filePathValue, &filePathDelete)

	err := createFiles(pm, files)
	if err != nil {
		return nil, err
	}

	return &RowGroup{
		PageManager:    pm,
		Metadata:       NewMetadata(rgIndex),
		TimestampChunk: chunk.NewTimestampChunk(pm.Config.PageSize, filePathTimestamp),
		ValueChunk:     chunk.NewValueChunk(pm.Config.PageSize, filePathValue),
		DeleteChunk:    chunk.NewDeleteChunk(pm.Config.PageSize, filePathDelete),
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
	err = rg.DeleteChunk.Add(rg.PageManager, false)
	if err != nil {
		return err
	}

	return nil
}

func (rg *RowGroup) Save() error {
	rg.Metadata.TimestampOffset = rg.TimestampChunk.CurrentOffset
	err := rg.TimestampChunk.Save(rg.PageManager)
	if err != nil {
		return err
	}

	rg.Metadata.ValueOffset = rg.ValueChunk.CurrentOffset
	err = rg.ValueChunk.Save(rg.PageManager)
	if err != nil {
		return err
	}

	rg.Metadata.DeleteOffset = rg.DeleteChunk.CurrentOffset
	err = rg.DeleteChunk.Save(rg.PageManager)
	if err != nil {
		return err
	}

	filePathMetadata := filepath.Join(rg.DirectoryPath, "metadata.db")
	err = rg.PageManager.WriteStructure(rg.Metadata.Serialize(), filePathMetadata, 0)
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
	deletePath := filepath.Join(path, "delete.db")

	rg := &RowGroup{
		PageManager:    pm,
		DirectoryPath:  path,
		TimestampChunk: nil,
		ValueChunk:     nil,
		DeleteChunk:    nil,
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

	deleteChunk := &chunk.DeleteChunk{
		ActivePage:    nil,
		FilePath:      deletePath,
		CurrentOffset: meta.DeleteOffset,
	}
	err = deleteChunk.Load(pm)
	if err != nil {
		return nil, err
	}

	rg.TimestampChunk = timestampChunk
	rg.ValueChunk = valueChunk
	rg.DeleteChunk = deleteChunk

	return rg, nil
}
