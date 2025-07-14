package memory

import (
	"time-series-engine/config"
	"time-series-engine/internal"
)

type MemTable struct {
	TimeSeries map[string]*internal.TimeSeries
	Points     []*internal.Point
	Size       uint64
}

func NewMemTable() *MemTable {
	return &MemTable{
		TimeSeries: make(map[string]*internal.TimeSeries),
		Points:     make([]*internal.Point, 0),
		Size:       0,
	}
}

func (mt *MemTable) IsFull(config *config.MemTableConfig) bool {
	return mt.Size >= config.MaxSize
}

func (mt *MemTable) Put(point *internal.Point) {
	// Check if points time series is already in hash map
	// if it is not, add new value to hash
	hash := point.TimeSeries.Hash()
	ts, exists := mt.TimeSeries[hash]
	if !exists {
		mt.TimeSeries[hash] = point.TimeSeries
		mt.Size += point.TimeSeries.Size()
	} else {
		point.TimeSeries = ts
	}

	mt.Points = append(mt.Points, point)
	mt.Size += 16 // size of one point (timestamp + value)
}

func (mt *MemTable) MinTimestamp() uint64 {
	return mt.Points[0].Timestamp
}

func (mt *MemTable) MaxTimestamp() uint64 {
	return mt.Points[len(mt.Points)-1].Timestamp
}
