package parquet

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/row_group"
)

type Metadata struct {
	StartTimestamp uint64
	EndTimestamp   uint64
	TimeSeries     *internal.TimeSeries
}

func NewMetadata(ts *internal.TimeSeries) *Metadata {
	return &Metadata{
		StartTimestamp: math.MaxUint64,
		EndTimestamp:   0,
		TimeSeries:     ts,
	}
}

type Parquet struct {
	Metadata       *Metadata
	ActiveRowGroup *row_group.RowGroup
	Config         *config.ParquetConfig
	PageManager    *page.Manager
	DirectoryPath  string
	PointsCounter  uint64
	RowGroupIndex  uint64
}

func NewParquet(ts *internal.TimeSeries, c *config.ParquetConfig, pm *page.Manager, path string) (*Parquet, error) {
	p := &Parquet{
		Metadata:       NewMetadata(ts),
		ActiveRowGroup: nil,
		Config:         c,
		PageManager:    pm,
		PointsCounter:  0,
		RowGroupIndex:  0,
		DirectoryPath:  path,
	}

	var err error
	p.ActiveRowGroup, err = row_group.NewRowGroup(ts, pm, p.createRowGroupDirectoryPath())
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Parquet) AddPoint(point *internal.Point) error {
	if point.Timestamp < p.Metadata.StartTimestamp {
		p.Metadata.StartTimestamp = point.Timestamp
		p.Metadata.EndTimestamp = point.Timestamp
	}

	var err error
	if p.PointsCounter != 0 && p.shouldFlushRowGroup() {
		p.ActiveRowGroup.Save(p.PageManager)

		p.RowGroupIndex++
		p.ActiveRowGroup, err = row_group.NewRowGroup(
			p.Metadata.TimeSeries, p.PageManager, p.createRowGroupDirectoryPath())
		if err != nil {
			return err
		}
	}

	p.ActiveRowGroup.AddPoint(point)
	p.Metadata.EndTimestamp = point.Timestamp
	p.PointsCounter++

	return nil
}

func (p *Parquet) Close() {
	if p.PointsCounter > 0 && p.ActiveRowGroup != nil {
		p.ActiveRowGroup.Save(p.PageManager)
	}
}

func (p *Parquet) createRowGroupDirectoryPath() string {
	rgName := fmt.Sprintf("rowgroup%04d.db", p.RowGroupIndex)
	rgPath := filepath.Join(p.DirectoryPath, rgName)
	err := os.MkdirAll(rgPath, 0644)
	if err != nil {
		return ""
	}

	return rgPath
}

func (p *Parquet) shouldFlushRowGroup() bool {
	return p.PointsCounter%p.Config.RowGroupSize == 0
}
