package tests

import (
	"testing"
	"time"
	"time-series-engine/internal"
	"time-series-engine/internal/memory"
)

func createTestPoint(ts string, value float64) *internal.Point {
	tags := internal.Tags{
		*internal.NewTag("host", "server1"),
	}
	timeSeries := internal.NewTimeSeries(ts, tags)
	point := internal.NewPoint(value, timeSeries)
	return &point
}

func TestWritePointWithFlush(t *testing.T) {
	mem := memory.NewMemTable(2)

	p1 := createTestPoint("cpu", 1.0)
	p2 := createTestPoint("cpu", 2.0)
	p3 := createTestPoint("cpu", 3.0)

	flush1 := mem.WritePointWithFlush(p1)
	if len(flush1) != 0 {
		t.Errorf("Expected no flush on first insert, got %d points", len(flush1))
	}

	flush2 := mem.WritePointWithFlush(p2)
	if len(flush2) != 0 {
		t.Errorf("Expected no flush on second insert, got %d points", len(flush2))
	}

	flush3 := mem.WritePointWithFlush(p3)
	if len(flush3) != 2 {
		t.Errorf("Expected flush of 2 points on third insert, got %d", len(flush3))
	}
}

func TestGetSortedPoints(t *testing.T) {
	mem := memory.NewMemTable(5)

	p1 := createTestPoint("cpu", 1.0)
	p2 := createTestPoint("cpu", 2.0)
	mem.WritePointWithFlush(p1)
	mem.WritePointWithFlush(p2)

	points, err := mem.GetSortedPoints(p1.TimeSeries)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 2 {
		t.Errorf("Expected 2 points, got %d", len(points))
	}
	if points[0].Value != 1.0 || points[1].Value != 2.0 {
		t.Error("Points are not sorted or values incorrect")
	}
}

func TestDeleteRange(t *testing.T) {
	mem := memory.NewMemTable(5)

	p1 := createTestPoint("cpu", 1.0)
	time.Sleep(1 * time.Second)
	p2 := createTestPoint("cpu", 2.0)
	time.Sleep(1 * time.Second)
	p3 := createTestPoint("cpu", 3.0)

	mem.WritePointWithFlush(p1)
	mem.WritePointWithFlush(p2)
	mem.WritePointWithFlush(p3)

	mem.DeleteRange(p1.TimeSeries, p2.Timestamp, p3.Timestamp)

	points, _ := mem.GetSortedPoints(p1.TimeSeries)
	if len(points) != 1 {
		t.Errorf("Expected 1 point after delete, got %d", len(points))
	}
	if points[0].Value != 1.0 {
		t.Errorf("Expected remaining point to have value 1.0")
	}
}

func TestMinAndMaxTimestamp(t *testing.T) {
	mem := memory.NewMemTable(5)

	p1 := createTestPoint("cpu", 1.0)
	time.Sleep(1 * time.Second)
	p2 := createTestPoint("cpu", 2.0)

	mem.WritePointWithFlush(p1)
	mem.WritePointWithFlush(p2)

	mint, err := mem.MinTimestamp(p1.TimeSeries)
	if err != nil || mint != p1.Timestamp {
		t.Errorf("Expected min timestamp %d, got %d", p1.Timestamp, mint)
	}

	maxt, err := mem.MaxTimestamp(p1.TimeSeries)
	if err != nil || maxt != p2.Timestamp {
		t.Errorf("Expected max timestamp %d, got %d", p2.Timestamp, maxt)
	}
}

func TestListTimeSeries(t *testing.T) {
	mem := memory.NewMemTable(5)

	p1 := createTestPoint("cpu", 1.0)
	p2 := createTestPoint("mem", 2.0)

	mem.WritePointWithFlush(p1)
	mem.WritePointWithFlush(p2)

	start := p1.Timestamp
	end := p2.Timestamp

	seriesMap := mem.ListTimeSeries(start, end)
	if len(seriesMap) != 2 {
		t.Errorf("Expected 2 series in range, got %d", len(seriesMap))
	}
	if len(seriesMap[p1.TimeSeries.Key()]) != 1 {
		t.Errorf("Expected 1 point in cpu series")
	}
	if len(seriesMap[p2.TimeSeries.Key()]) != 1 {
		t.Errorf("Expected 1 point in mem series")
	}
}
