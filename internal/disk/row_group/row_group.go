package row_group

import (
	"time-series-engine/internal"
	"time-series-engine/internal/disk/chunk"
	"time-series-engine/internal/disk/page"
)

type RowGroup struct {
	Metadata       *Metadata
	TimestampChunk *chunk.TimestampChunk
	ValueChunk     *chunk.ValueChunk
	TagsChunks     map[string]*chunk.StringChunk
	PointsNumber   uint64
}

func NewRowGroup(ts *internal.TimeSeries, pageSize uint64) *RowGroup {
	tags := make(map[string]*chunk.StringChunk)
	for _, tag := range ts.Tags {
		tags[tag.Name] = chunk.NewStringChunk(pageSize)
	}

	return &RowGroup{
		Metadata:       NewMetadata(),
		TimestampChunk: chunk.NewTimestampChunk(pageSize),
		ValueChunk:     chunk.NewValueChunk(pageSize),
		TagsChunks:     tags,
		PointsNumber:   0,
	}
}

func (rg *RowGroup) AddPoint(pm *page.Manager, p *internal.Point) {
	rg.Metadata.Update(p)

	rg.TimestampChunk.Add(pm, p.Timestamp)
	rg.ValueChunk.Add(pm, p.Value)

	for _, tag := range p.TimeSeries.Tags {
		tagChunk, _ := rg.TagsChunks[tag.Name]
		tagChunk.Add(pm, tag.Value)
	}

	rg.PointsNumber++
}
