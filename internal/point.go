package internal

import (
	"time"
)

type Point struct {
	Timestamp  uint64
	Value      float64
	TimeSeries *TimeSeries
}

func NewPoint(value float64, timeSeries *TimeSeries) Point {
	return Point{
		Timestamp:  calculateTimestamp(),
		Value:      value,
		TimeSeries: timeSeries,
	}
}

func calculateTimestamp() uint64 {
	return uint64(time.Now().Unix())
}
