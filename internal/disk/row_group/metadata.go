package row_group

import (
	"math"
	"time-series-engine/internal"
)

type Metadata struct {
	MinTimestamp uint64
	MaxTimestamp uint64

	MinMeasurementName string
	MaxMeasurementName string

	MinValue float64
	MaxValue float64

	MinTags map[string]string
	MaxTags map[string]string
}

func NewMetadata() *Metadata {
	return &Metadata{
		MinTimestamp: ^uint64(0), // max uint64 (all bits are 1)
		MaxTimestamp: 0,

		MinMeasurementName: "",
		MaxMeasurementName: "",

		MinValue: math.Inf(1),
		MaxValue: math.Inf(-1),

		MinTags: make(map[string]string),
		MaxTags: make(map[string]string),
	}
}

func (m *Metadata) Update(p *internal.Point) {
	if p.Timestamp < m.MinTimestamp {
		p.Timestamp = m.MinTimestamp
	}
	if p.Timestamp > m.MaxTimestamp {
		m.MaxTimestamp = p.Timestamp
	}

	if p.Value < m.MinValue {
		p.Value = m.MinValue
	}
	if p.Value > m.MaxValue {
		m.MaxValue = p.Value
	}

	if m.MinMeasurementName == "" {
		m.MinMeasurementName = p.TimeSeries.MeasurementName
		m.MaxMeasurementName = p.TimeSeries.MeasurementName
	} else {
		if p.TimeSeries.MeasurementName < m.MinMeasurementName {
			m.MinMeasurementName = p.TimeSeries.MeasurementName
		}
		if p.TimeSeries.MeasurementName > m.MaxMeasurementName {
			m.MaxMeasurementName = p.TimeSeries.MeasurementName
		}
	}

	for _, tag := range p.TimeSeries.Tags {
		minName, found := m.MinTags[tag.Name]
		if !found {
			m.MinTags[tag.Name] = tag.Value
		} else if tag.Value < minName {
			m.MinTags[tag.Name] = tag.Value
		}

		maxName, found := m.MaxTags[tag.Name]
		if !found {
			m.MaxTags[tag.Name] = tag.Value
		} else if tag.Value > maxName {
			m.MaxTags[tag.Name] = tag.Value
		}
	}
}
