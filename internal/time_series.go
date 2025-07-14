package internal

import (
	"crypto/sha256"
	"fmt"
)

type TimeSeries struct {
	MeasurementName string
	Tags            Tags
}

func NewTimeSeries(measurementName string, tags Tags) *TimeSeries {
	tags.Sort()
	return &TimeSeries{
		MeasurementName: measurementName,
		Tags:            tags,
	}
}

func (ts *TimeSeries) Hash() string {
	hasher := sha256.New()
	hasher.Write([]byte(ts.MeasurementName))

	for _, tag := range ts.Tags {
		hasher.Write([]byte(tag.Name))
		hasher.Write([]byte(tag.Value))
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (ts *TimeSeries) Size() uint64 {
	size := uint64(len(ts.MeasurementName))
	for _, tag := range ts.Tags {
		size += uint64(len(tag.Name))
		size += uint64(len(tag.Value))
	}

	return size
}
