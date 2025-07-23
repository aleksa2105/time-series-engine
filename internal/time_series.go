package internal

import (
	"fmt"
	"strings"
)

type TimeSeries struct {
	MeasurementName string
	Tags            Tags
}

func NewTimeSeries(measurementName string, tags Tags) *TimeSeries {
	return &TimeSeries{
		MeasurementName: measurementName,
		Tags:            tags,
	}
}

func (ts *TimeSeries) Hash() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(ts.MeasurementName)
	// Add sorted tags:
	ts.Tags.Sort()
	for _, tag := range ts.Tags {
		stringBuilder.WriteString(fmt.Sprintf("|%s=%s", tag.Name, tag.Value))
	}

	return stringBuilder.String()
}
