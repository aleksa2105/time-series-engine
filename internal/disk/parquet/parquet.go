package parquet

import (
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
		StartTimestamp: 0,
		EndTimestamp:   0,
		TimeSeries:     ts,
	}
}

type Parquet struct {
	Metadata       *Metadata
	ActiveRowGroup *row_group.RowGroup
	Config         *config.ParquetConfig
	PageManager    *page.Manager
	Counter        uint64
}

func NewParquet(ts *internal.TimeSeries, c *config.ParquetConfig, pm *page.Manager) *Parquet {
	return &Parquet{
		Metadata:       NewMetadata(ts),
		ActiveRowGroup: row_group.NewRowGroup(ts, pm.Config.PageSize),
		Config:         c,
		PageManager:    pm,
		Counter:        0,
	}
}

func (p *Parquet) AddPoint(point *internal.Point) {
	if p.Metadata.StartTimestamp == 0 {
		p.Metadata.StartTimestamp = point.Timestamp
		p.Metadata.EndTimestamp = point.Timestamp
	}

	if p.Counter == p.Config.RowGroupSize {
		// upis row groupa na disk

		p.ActiveRowGroup = row_group.NewRowGroup(p.Metadata.TimeSeries, p.Config.PageSize)
	}

	p.ActiveRowGroup.AddPoint(p.PageManager, point)
}
