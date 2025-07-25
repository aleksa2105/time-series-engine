package internal

import (
	"fmt"
	"strings"
)

type TimeSeries struct {
	MeasurementName string
	Tags            Tags
	Hash            string
}

func NewTimeSeries(measurementName string, tags Tags) *TimeSeries {
	ts := &TimeSeries{
		MeasurementName: measurementName,
		Tags:            tags,
		Hash:            "",
	}

	ts.Hash = ts.hash()
	return ts
}

func (ts *TimeSeries) hash() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(ts.MeasurementName)
	// Add sorted tags:
	ts.Tags.Sort()
	for _, tag := range ts.Tags {
		stringBuilder.WriteString(fmt.Sprintf("|%s=%s", tag.Name, tag.Value))
	}

	return stringBuilder.String()
}
