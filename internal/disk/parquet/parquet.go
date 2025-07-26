package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/page/page_manager"
	"time-series-engine/internal/disk/row_group"
)

type Parquet struct {
	Metadata       *Metadata
	ActiveRowGroup *row_group.RowGroup
	Config         *config.ParquetConfig
	PageManager    *page_manager.Manager
	DirectoryPath  string
	RowGroupIndex  uint64
}

func NewParquet(timeSeriesHash string, c *config.ParquetConfig, pm *page_manager.Manager, dirPath string) (*Parquet, error) {
	p := &Parquet{
		Metadata:       NewMetadata(timeSeriesHash),
		ActiveRowGroup: nil,
		Config:         c,
		PageManager:    pm,
		RowGroupIndex:  0,
		DirectoryPath:  dirPath,
	}

	var err error
	var path string
	path, err = p.createRowGroupDirectoryPath()
	if err != nil {
		return nil, err
	}

	p.ActiveRowGroup, err = row_group.NewRowGroup(pm, path, p.RowGroupIndex)
	if err != nil {
		return nil, err
	}

	err = pm.CreateFile(filepath.Join(dirPath, "metadata.db"))
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Parquet) AddPoint(point *internal.Point) error {
	p.Metadata.Update(point.Timestamp)

	var err error
	if p.shouldFlushRowGroup() {
		err = p.ActiveRowGroup.Save()
		if err != nil {
			return err
		}

		p.RowGroupIndex++
		var path string
		path, err = p.createRowGroupDirectoryPath()
		if err != nil {
			return err
		}

		p.ActiveRowGroup, err = row_group.NewRowGroup(p.PageManager, path, p.RowGroupIndex)
		if err != nil {
			return err
		}
	}

	err = p.ActiveRowGroup.AddPoint(point)
	if err != nil {
		return err
	}

	return nil
}

func (p *Parquet) Close() error {
	err := p.ActiveRowGroup.Save()
	if err != nil {
		return err
	}

	filePathMetadata := filepath.Join(p.DirectoryPath, "metadata.db")
	err = p.PageManager.WriteStructure(p.Metadata.Serialize(), filePathMetadata, 0)
	if err != nil {
		return err
	}

	return nil
}

func (p *Parquet) createRowGroupDirectoryPath() (string, error) {
	rgName := fmt.Sprintf("rowgroup%04d", p.RowGroupIndex)
	rgPath := filepath.Join(p.DirectoryPath, rgName)
	err := os.MkdirAll(rgPath, 0755)
	if err != nil {
		return "", err
	}

	return rgPath, nil
}

func (p *Parquet) shouldFlushRowGroup() bool {
	return p.Metadata.PointsNumber != 0 && p.Metadata.PointsNumber%p.Config.RowGroupSize == 0
}

func LoadParquet(m *Metadata, c *config.ParquetConfig, pm *page_manager.Manager, path string) (*Parquet, error) {
	p := &Parquet{
		Metadata:       m,
		ActiveRowGroup: nil,
		Config:         c,
		PageManager:    pm,
		DirectoryPath:  path,
		RowGroupIndex:  0,
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	if len(entries) > 0 {
		rgPath := filepath.Join(path, entries[len(entries)-1].Name())
		p.ActiveRowGroup, err = row_group.LoadRowGroup(pm, rgPath)
		p.RowGroupIndex = uint64(len(entries)) - 1
	} else {
		rgPath, err := p.createRowGroupDirectoryPath()
		if err != nil {
			return nil, err
		}

		p.ActiveRowGroup, err = row_group.NewRowGroup(pm, rgPath, p.RowGroupIndex)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}
