package tests

import (
	"testing"
	"time-series-engine/internal"
)

func TestNewTimeSeries(t *testing.T) {
	tags := internal.Tags{
		{Name: "b", Value: "2"},
		{Name: "a", Value: "1"},
	}

	ts := internal.NewTimeSeries("cpu", tags)

	if ts.MeasurementName != "cpu" {
		t.Errorf("expected measurement name cpu, got %s", ts.MeasurementName)
	}

	// Tags should be sorted
	if ts.Tags[0].Name != "a" {
		t.Errorf("expected first tag to be a, got %s", ts.Tags[0].Name)
	}
}

func TestTimeSeriesHash_DifferentTagsDifferentHash(t *testing.T) {
	tags1 := internal.Tags{
		{Name: "a", Value: "1"},
		{Name: "b", Value: "2"},
	}
	tags2 := internal.Tags{
		{Name: "a", Value: "1"},
		{Name: "b", Value: "3"},
	}

	ts1 := internal.NewTimeSeries("cpu", tags1)
	ts2 := internal.NewTimeSeries("cpu", tags2)

	hash1 := ts1.Hash()
	hash2 := ts2.Hash()

	if hash1 == hash2 {
		t.Errorf("expected different hashes for different tags, got same hash %s", hash1)
	}
}

func TestTimeSeriesSize(t *testing.T) {
	tags := internal.Tags{
		{Name: "env", Value: "prod"},
		{Name: "host", Value: "server1"},
	}

	ts := internal.NewTimeSeries("cpu", tags)

	expectedSize := uint64(len("cpu") +
		len("env") + len("prod") +
		len("host") + len("server1"))

	if ts.Size() != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, ts.Size())
	}
}
