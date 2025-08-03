package disk

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/parquet"
	"time-series-engine/internal/disk/row_group"
)

func Get(pm *page.Manager, windowsDir string, ts *internal.TimeSeries, minTimestamp uint64, maxTimestamp uint64) ([]*internal.Point, error) {
	windows, err := os.ReadDir(windowsDir)
	result := make([]*internal.Point, 0)
	if err != nil {
		return nil, errors.New("[ERROR]: cannot read from time windows directory")
	}

	for _, window := range windows {
		start, end, err := MinMaxTimestamp(window.Name())
		if err != nil {
			return nil, err
		}

		if DoIntervalsOverlap(minTimestamp, maxTimestamp, start, end) {
			p, err := GetParquet(pm, windowsDir, window.Name(), ts, minTimestamp, maxTimestamp)
			if err != nil {
				return nil, err
			}

			if p != "" {
				items, err := GetInParquet(pm, p, minTimestamp, maxTimestamp)
				result = append(result, items...)
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		break
	}

	return result, nil
}

func MinMaxTimestamp(name string) (uint64, uint64, error) {
	re := regexp.MustCompile(`^window_(\d+)-(\d+)$`)
	matches := re.FindStringSubmatch(name)

	if len(matches) == 3 {
		start, err1 := strconv.ParseUint(matches[1], 10, 64)
		end, err2 := strconv.ParseUint(matches[2], 10, 64)

		if err1 != nil || err2 != nil {
			return 0, 0, errors.New("[ERROR]: cannot parse timestamp")
		}

		return start, end, nil
	} else {
		return 0, 0, errors.New("[ERROR]: cannot parse timestamp")
	}
}

func GetParquet(
	pm *page.Manager,
	windowsDir string, windowDir string,
	ts *internal.TimeSeries, minTimestamp uint64, maxTimestamp uint64,
) (string, error) {
	parquets, err := os.ReadDir(filepath.Join(windowsDir, windowDir))
	if err != nil {
		return "", errors.New("[ERROR]: cannot read from time windows directory")
	}

	for _, p := range parquets {
		pPath := filepath.Join(windowsDir, windowDir, p.Name())
		metaPath := filepath.Join(pPath, "metadata.db")

		data, err := pm.ReadStructure(metaPath, 0)
		if err != nil {
			return "", err
		}

		meta, err := parquet.DeserializeParquetMetadata(data)
		if err != nil {
			return "", err
		}

		if meta.TimeSeriesHash != ts.Hash ||
			!DoIntervalsOverlap(minTimestamp, maxTimestamp, meta.MinTimestamp, meta.MaxTimestamp) {
			continue
		}

		return pPath, nil
	}

	return "", nil
}

func GetInParquet(pm *page.Manager, parquetPath string, minTimestamp uint64, maxTimestamp uint64) ([]*internal.Point, error) {
	rowGroups, err := os.ReadDir(parquetPath)
	result := make([]*internal.Point, 0)
	if err != nil {
		return nil, fmt.Errorf("[ERROR]: cannot read from parquet directory: %s", parquetPath)
	}

	for _, rg := range rowGroups {
		if !rg.IsDir() {
			continue
		}

		rgPath := filepath.Join(parquetPath, rg.Name())
		metaPath := filepath.Join(rgPath, "metadata.db")

		metaBytes, err := pm.ReadStructure(metaPath, 0)
		if err != nil {
			return nil, err
		}

		meta, err := row_group.DeserializeMetadata(metaBytes)
		if err != nil {
			return nil, err
		}

		if DoIntervalsOverlap(minTimestamp, maxTimestamp, meta.MinTimestamp, meta.MaxTimestamp) {
			items, err := GetInRowGroup(pm, rgPath, minTimestamp, maxTimestamp)
			result = append(result, items...)
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func DoIntervalsOverlap(min1, max1, min2, max2 uint64) bool {
	return min1 <= max2 && max1 >= min2
}

func GetInRowGroup(
	pm *page.Manager, rgPath string,
	minTimestamp uint64, maxTimestamp uint64,
) ([]*internal.Point, error) {
	result := make([]*internal.Point, 0)
	tsPath := filepath.Join(rgPath, "timestamp.db")
	valuePath := filepath.Join(rgPath, "value.db")
	deletePath := filepath.Join(rgPath, "delete.db")

	tsIter, err := NewIterator(pm, tsPath, Timestamp)
	if err != nil {
		return nil, err
	}
	skipped, err := tsIter.Skip(minTimestamp, maxTimestamp)
	if err != nil {
		return nil, err
	}

	valueIter, err := NewIterator(pm, valuePath, Value)
	if err != nil {
		return nil, err
	}
	err = valueIter.Advance(skipped)
	if err != nil {
		return nil, err
	}

	deleteIter, err := NewIterator(pm, deletePath, Delete)
	if err != nil {
		return nil, err
	}
	err = deleteIter.Advance(skipped)
	if err != nil {
		return nil, err
	}

	for {
		e, err := tsIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tsEntry := e.(*entry.TimestampEntry)
		if tsEntry.GetValue() > maxTimestamp {
			break
		}

		e, err = valueIter.Next()
		if err != nil {
			return nil, err
		}
		valueEntry := e.(*entry.ValueEntry)

		e, err = deleteIter.Next()
		if err != nil {
			return nil, err
		}
		deleteEntry := e.(*entry.DeleteEntry)

		if !deleteEntry.Deleted {
			p := internal.Point{
				Value:     valueEntry.Value,
				Timestamp: tsEntry.Value,
			}
			result = append(result, &p)
		}
	}

	return result, nil
}

func Aggregate(ts *internal.TimeSeries, minTimestamp uint64, maxTimestamp uint64, pm *page.Manager, windowsDir string, function string) (float64, uint64, error) {
	var result float64
	var sumValue float64
	var pointsNumber uint64

	switch function {
	case "Min":
		result = math.MaxFloat64
	case "Max":
		result = -math.MaxFloat64
	case "Average":
		result = 0
	}

	windows, err := os.ReadDir(windowsDir)
	if err != nil {
		return 0, 0, err
	}

	for _, window := range windows {
		windowName := window.Name()
		parquets, err := os.ReadDir(filepath.Join(windowsDir, windowName))
		if err != nil {
			return 0, 0, err
		}

		for _, p := range parquets {
			pPath := filepath.Join(windowsDir, windowName, p.Name())
			metaPath := filepath.Join(pPath, "metadata.db")

			data, err := pm.ReadStructure(metaPath, 0)
			if err != nil {
				return 0, 0, err
			}

			meta, err := parquet.DeserializeParquetMetadata(data)
			if err != nil {
				return 0, 0, err
			}

			if meta.TimeSeriesHash != ts.Hash {
				continue
			}
			if !DoIntervalsOverlap(minTimestamp, maxTimestamp, meta.MinTimestamp, meta.MaxTimestamp) {
				continue
			}

			items, err := GetInParquet(pm, pPath, minTimestamp, maxTimestamp)
			if err != nil {
				return 0, 0, err
			}
			if len(items) == 0 {
				continue
			}

			sum := insertionSort(items)

			switch function {
			case "Min":
				first := items[0].Value
				if first < result {
					result = first
				}
			case "Max":
				last := items[len(items)-1].Value
				if last > result {
					result = last
				}
			case "Average":
				pointsNumber += uint64(len(items))
				sumValue += sum
			}
		}
	}

	if function == "Average" {
		if pointsNumber == 0 {
			return 0, 0, nil
		}
		return sumValue, pointsNumber, nil
	}

	return result, pointsNumber, nil
}

func insertionSort(array []*internal.Point) float64 {
	var sum float64
	for i := 0; i < len(array); i++ {
		key := array[i]
		j := i - 1

		for j >= 0 && array[j].Value > key.Value {
			array[j+1] = array[j]
			j--
		}
		array[j+1] = key
		sum += key.Value
	}
	return sum
}
