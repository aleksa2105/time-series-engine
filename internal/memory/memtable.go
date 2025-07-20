package memory

import (
	"fmt"
	"time-series-engine/internal"
)

type MemTable struct {
	Data    map[string]*DoublyLinkedList
	MaxSize uint64
}

func NewMemTable(maxSize uint64) *MemTable {
	return &MemTable{
		Data:    make(map[string]*DoublyLinkedList),
		MaxSize: maxSize,
	}
}

/*
	TODO: ako budemo dozvoljavali upis duplikata ili vracanje blago u proslost
	TODO: ne toliko da izadjemo iz trenutnog time windowa, onda bi trebalo umjesto
	TODO: spregnute liste da se koristi neko od stabla (AVL ili RedBlack), to jest sortirani skup
*/

func (mt *MemTable) WritePointWithFlush(point *internal.Point) []*internal.Point {
	timeSeriesKey := point.TimeSeries.Key()
	storage, exists := mt.Data[timeSeriesKey]
	if !exists {
		mt.Data[timeSeriesKey] = NewDoublyLinkedList(mt.MaxSize)
		storage = mt.Data[timeSeriesKey]
	}

	var pointsToFlush []*internal.Point = nil
	if storage.IsFull() {
		pointsToFlush = storage.GetSortedPoints()

		storage = NewDoublyLinkedList(mt.MaxSize)
		storage.Insert(point)
	} else {
		storage.Insert(point)
	}

	return pointsToFlush
}

func (mt *MemTable) DeleteRange(timeSeries *internal.TimeSeries, minTimestamp, maxTimestamp uint64) {
	storage, exists := mt.Data[timeSeries.Key()]
	if !exists {
		return
	}
	storage.DeleteRange(minTimestamp, maxTimestamp)
}
func (mt *MemTable) ListTimeSeries(minTimestamp, maxTimestamp uint64) map[string][]*internal.Point {
	allTimeSeries := make(map[string][]*internal.Point)
	for timeSeriesKey, storage := range mt.Data {
		points := storage.GetPointsInInterval(minTimestamp, maxTimestamp)
		allTimeSeries[timeSeriesKey] = points
	}
	return allTimeSeries
}
func (mt *MemTable) GetSortedPoints(timeSeries *internal.TimeSeries) ([]*internal.Point, error) {
	timeSeriesKey := timeSeries.Key()
	storage, exists := mt.Data[timeSeriesKey]
	if !exists {
		return nil, fmt.Errorf("there are no stored points of that time series")
	}
	return storage.GetSortedPoints(), nil
}

func (mt *MemTable) MinTimestamp(timeSeries *internal.TimeSeries) (uint64, error) {
	storage, exits := mt.Data[timeSeries.Key()]
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
	storage, exits := mt.Data[timeSeries.Key()]
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
