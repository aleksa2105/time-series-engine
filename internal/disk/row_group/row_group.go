package row_group

import (
	"time-series-engine/internal"
	"time-series-engine/internal/disk/chunk"
	"time-series-engine/internal/disk/page"
)

type RowGroup struct {
	Metadata         *Metadata
	TimestampChunk   *chunk.TimestampChunk
	ValueChunk       *chunk.ValueChunk
	MeasurementChunk *chunk.StringChunk
	TagsChunks       map[string]*chunk.StringChunk
	PointsNumber     uint64
}

func NewRowGroup(pageSize uint64) *RowGroup {
	return &RowGroup{
		Metadata:         NewMetadata(),
		TimestampChunk:   chunk.NewTimestampChunk(pageSize),
		ValueChunk:       chunk.NewValueChunk(pageSize),
		MeasurementChunk: chunk.NewStringChunk(pageSize),
		TagsChunks:       make(map[string]*chunk.StringChunk),
		PointsNumber:     0,
	}
}

func (rg *RowGroup) AddPoint(pm *page.Manager, p *internal.Point) {
	rg.Metadata.Update(p)

	rg.TimestampChunk.Add(pm, p.Timestamp)
	rg.ValueChunk.Add(pm, p.Value)
	rg.MeasurementChunk.Add(pm, p.TimeSeries.MeasurementName)
	rg.AddTags(pm, p)

	rg.PointsNumber++
}

func (rg *RowGroup) AddTags(pm *page.Manager, p *internal.Point) {
	visited := make(map[string]bool)

	for key := range rg.TagsChunks {
		visited[key] = false
	}

	for _, tag := range p.TimeSeries.Tags {
		tagChunk, found := rg.TagsChunks[tag.Name]
		if !found {
			rg.TagsChunks[tag.Name] = chunk.NewStringChunk(pm.Config.PageSize)
			rg.TagsChunks[tag.Name].AddNullEntries(rg.PointsNumber)
		} else {
			tagChunk.Add(pm, tag.Value)
			visited[tag.Name] = true
		}
	}

	for key, value := range visited {
		if !value {
			rg.TagsChunks[key].AddNullEntry(pm)
		}
	}
}
