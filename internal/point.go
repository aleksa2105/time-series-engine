package internal

type Point struct {
	Timestamp uint64
	Value     float64
}

func NewPoint(timestamp uint64, value float64) Point {
	return Point{
		Timestamp: timestamp,
		Value:     value,
	}
}

/*
func calculateTimestamp() uint64 {
	return uint64(time.Now().Unix())
}
*/
