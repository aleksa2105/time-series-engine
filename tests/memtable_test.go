package tests

import (
	"testing"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/memory"
)

func TestNewMemTable(t *testing.T) {
	mt := memory.NewMemTable()

	if mt == nil {
		t.Fatal("expected non-nil MemTable")
	}

	if len(mt.Points) != 0 {
		t.Errorf("expected empty Points slice, got %d elements", len(mt.Points))
	}

	if mt.Size != 0 {
		t.Errorf("expected size 0, got %d", mt.Size)
	}
}

func TestMemTablePutAndIsFull(t *testing.T) {
	mt := memory.NewMemTable()

	ts := internal.NewTimeSeries("cpu", internal.Tags{
		{Name: "env", Value: "prod"},
	})

	point := internal.NewPoint(1.23, ts)
	mt.Put(&point)

	if len(mt.Points) != 1 {
		t.Errorf("expected 1 point, got %d", len(mt.Points))
	}

	if mt.TimeSeries[ts.Hash()] == nil {
		t.Errorf("expected timeseries to exist in TimeSeries map")
	}

	// check Size includes time series size + 16 bytes for point
	expectedSize := ts.Size() + 16
	if mt.Size != expectedSize {
		t.Errorf("expected memtable size %d, got %d", expectedSize, mt.Size)
	}

	conf := &config.MemTableConfig{MaxSize: expectedSize}
	if !mt.IsFull(conf) {
		t.Errorf("expected MemTable to be full")
	}
}

func TestMemTableMinMaxTimestamp(t *testing.T) {
	mt := memory.NewMemTable()

	ts := internal.NewTimeSeries("cpu", internal.Tags{})

	p1 := internal.NewPoint(10.0, ts)
	p1.Timestamp = 100

	p2 := internal.NewPoint(20.0, ts)
	p2.Timestamp = 200

	mt.Put(&p1)
	mt.Put(&p2)

	if mt.MinTimestamp() != 100 {
		t.Errorf("expected min timestamp 100, got %d", mt.MinTimestamp())
	}

	if mt.MaxTimestamp() != 200 {
		t.Errorf("expected max timestamp 200, got %d", mt.MaxTimestamp())
	}
}
