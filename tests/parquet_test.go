package tests

import (
	"fmt"
	"testing"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/page/page_manager"
	"time-series-engine/internal/disk/parquet"
)

func TestParquet(t *testing.T) {
	tag1 := internal.NewTag("location", "belgrade")
	tag2 := internal.NewTag("sensor ID", "a1")
	tags := internal.Tags{}
	tags = append(tags, *tag1)
	tags = append(tags, *tag2)

	ts := internal.NewTimeSeries("temperature", tags)
	c := config.LoadConfiguration()
	pm := page_manager.NewManager(c.PageConfig)

	p, err := parquet.NewParquet(
		ts, &c.ParquetConfig, pm, "./internal/disk/data/parquet1")
	if err != nil {
		fmt.Println("Parquet making error")
	}

	point1 := internal.NewPoint(100, ts)
	point1.Timestamp = 1

	point2 := internal.NewPoint(200, ts)
	point2.Timestamp = 2

	point3 := internal.NewPoint(300, ts)
	point3.Timestamp = 3

	point4 := internal.NewPoint(400, ts)
	point4.Timestamp = 4

	point5 := internal.NewPoint(500, ts)
	point5.Timestamp = 5

	point6 := internal.NewPoint(600, ts)
	point6.Timestamp = 6

	point7 := internal.NewPoint(700, ts)
	point7.Timestamp = 7

	if p != nil {
		err = p.AddPoint(&point1)
		if err != nil {
			fmt.Println("Parquet add point1 failed")
		}

		err = p.AddPoint(&point2)
		if err != nil {
			fmt.Println("Parquet add point2 failed")
		}

		err = p.AddPoint(&point3)
		if err != nil {
			fmt.Println("Parquet add point3 failed")
		}

		err = p.AddPoint(&point4)
		if err != nil {
			fmt.Println("Parquet add point4 failed")
		}

		err = p.AddPoint(&point5)
		if err != nil {
			fmt.Println("Parquet add point5 failed")
		}

		err = p.AddPoint(&point6)
		if err != nil {
			fmt.Println("Parquet add point6 failed")
		}

		err = p.AddPoint(&point7)
		if err != nil {
			fmt.Println("Parquet add point7 failed")
		}

		p.Close()
	}
}
