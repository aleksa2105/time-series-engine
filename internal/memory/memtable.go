package memory

import (
	"fmt"
	"time-series-engine/internal"
)

type MemTable struct {
	Data            map[string]*DoublyLinkedList
	Count           uint64
	MaxSize         uint64
	StartWALSegment string
	StartWALOffset  uint64
}

func NewMemTable(maxSize uint64) *MemTable {
	return &MemTable{
		Data:    make(map[string]*DoublyLinkedList),
		MaxSize: maxSize,
		Count:   0,
	}
}

func (mt *MemTable) WritePointWithFlush(timeSeries *internal.TimeSeries, point *internal.Point) []*internal.Point {
	timeSeriesKey := timeSeries.Hash

	storage, exists := mt.Data[timeSeriesKey]
	if !exists {
		mt.Data[timeSeriesKey] = NewDoublyLinkedList()
		storage = mt.Data[timeSeriesKey]
	}

	var pointsToFlush []*internal.Point = nil
	if mt.IsFull() {
		pointsToFlush = storage.GetSortedPoints()
		storage = NewDoublyLinkedList()
		mt.Count = 0
	}

	storage.Insert(point)
	mt.Count += 1

	return pointsToFlush
}

func (mt *MemTable) IsFull() bool {
	return mt.Count == mt.MaxSize
}

func (mt *MemTable) FlushAllTimeSeries() map[string][]*internal.Point {
	allTimeSeries := make(map[string][]*internal.Point)

	for tsKey, storage := range mt.Data {
		allTimeSeries[tsKey] = storage.GetSortedPoints()
		storage = NewDoublyLinkedList()
	}
	mt.Count = 0

	return allTimeSeries
}

func (mt *MemTable) DeleteRange(timeSeries *internal.TimeSeries, minTimestamp, maxTimestamp uint64) {
	storage, exists := mt.Data[timeSeries.Hash]
	if !exists {
		return
	}
	mt.Count -= storage.DeleteRange(minTimestamp, maxTimestamp)
}

func (mt *MemTable) List(timeSeries *internal.TimeSeries, minTimestamp, maxTimestamp uint64) []*internal.Point {
	timeSeriesKey := timeSeries.Hash
	storage, exists := mt.Data[timeSeriesKey]
	if !exists {
		return nil
	}
	return storage.GetPointsInInterval(minTimestamp, maxTimestamp)
}

func (mt *MemTable) AggregateMinMax(
	ts *internal.TimeSeries,
	minTimestamp, maxTimestamp uint64,
	isMin bool,
) (float64, bool) {
	storage, exists := mt.Data[ts.Hash]
	if !exists {
		return 0.0, false
	}
	points := storage.GetPointsInInterval(minTimestamp, maxTimestamp)
	if points == nil {
		return 0.0, false
	}

	if isMin {
		return points[0].Value, true
	}
	return points[len(points)-1].Value, true
}

func (mt *MemTable) GetSortedPoints(timeSeries *internal.TimeSeries) ([]*internal.Point, error) {
	timeSeriesKey := timeSeries.Hash
	storage, exists := mt.Data[timeSeriesKey]
	if !exists {
		return nil, fmt.Errorf("there are no stored points of that time series")
	}
	return storage.GetSortedPoints(), nil
}

func (mt *MemTable) MinTimestamp(timeSeries *internal.TimeSeries) (uint64, error) {
	storage, exits := mt.Data[timeSeries.Hash]
	if !exits {
		return 0, fmt.Errorf("there are no stored points of that time series")
	}

	if storage.IsEmpty() {
		return 0, fmt.Errorf("there are no stored points of that time series yet")
	}

	point, err := storage.FirstPoint()
	if err != nil {
		return 0, err
	}
	return point.Timestamp, nil
}

func (mt *MemTable) MaxTimestamp(timeSeries *internal.TimeSeries) (uint64, error) {
	storage, exits := mt.Data[timeSeries.Hash]
	if !exits {
		return 0, fmt.Errorf("there are no stored points of that time series")
	}

	if storage.IsEmpty() {
		return 0, fmt.Errorf("there are no stored points of that time series yet")
	}

	point, err := storage.LastPoint()
	if err != nil {
		return 0, err
	}
	return point.Timestamp, nil
}
