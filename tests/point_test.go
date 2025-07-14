package tests

import (
	"testing"
	"time-series-engine/internal"
)

func TestNewPoint(t *testing.T) {
	tags := internal.Tags{
		{Name: "env", Value: "prod"},
	}

	ts := internal.NewTimeSeries("cpu", tags)

	point := internal.NewPoint(42.0, ts)

	if point.Value != 42.0 {
		t.Errorf("expected value 42.0, got %f", point.Value)
	}

	if point.TimeSeries.MeasurementName != "cpu" {
		t.Errorf("expected measurement name cpu, got %s", point.TimeSeries.MeasurementName)
	}

	if point.Timestamp == 0 {
		t.Errorf("expected timestamp to be non-zero")
	}
}
