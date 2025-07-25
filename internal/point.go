package internal

import "time"

type Point struct {
	Timestamp uint64
	Value     float64
}

func NewPoint(value float64) *Point {
	return &Point{
		Timestamp: calculateTimestamp(),
		Value:     value,
	}
}

func calculateTimestamp() uint64 {
	return uint64(time.Now().Unix())
}
