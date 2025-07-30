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

func (mt *MemTable) WritePointWithFlush(timeSeries *internal.TimeSeries, point *internal.Point) map[string][]*internal.Point {
	storage, exists := mt.Data[timeSeries.Hash]
	if !exists {
		mt.Data[timeSeries.Hash] = NewDoublyLinkedList()
		storage = mt.Data[timeSeries.Hash]
	}

	storage.Insert(point)
	mt.Count += 1

	if mt.IsFull() {
		return mt.FlushAllTimeSeries()
	}
	return nil
}

func (mt *MemTable) IsFull() bool {
	return mt.Count == mt.MaxSize
}

func (mt *MemTable) FlushAllTimeSeries() map[string][]*internal.Point {
	allTimeSeries := make(map[string][]*internal.Point)

	for tsHash, storage := range mt.Data {
		allTimeSeries[tsHash] = storage.GetSortedPoints()
	}
	mt.Count = 0
	mt.Data = make(map[string]*DoublyLinkedList)

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

func (mt *MemTable) Aggregate(
	ts *internal.TimeSeries,
	minTimestamp, maxTimestamp uint64,
	function string,
) (float64, uint64, bool) {
	storage, exists := mt.Data[ts.Hash]
	var sum float64
	if !exists {
		return 0.0, 0, false
	}
	points := storage.GetPointsInInterval(minTimestamp, maxTimestamp)
	if len(points) == 0 {
		return 0.0, 0, false
	}

	switch function {
	case "Min":
		return points[0].Value, 0, true
	case "Max":
		return points[len(points)-1].Value, 0, true
	case "Average":
		for _, point := range points {
			sum += point.Value
		}
		return sum, uint64(len(points)), true
	}
	return 0.0, 0, false
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
