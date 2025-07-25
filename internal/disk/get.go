package disk

import (
	"errors"
	"fmt"
	"io"
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

func Get(pm *page.Manager, windowsDir string, ts *internal.TimeSeries, minTimestamp uint64, maxTimestamp uint64) error {
	windows, err := os.ReadDir(windowsDir)
	if err != nil {
		return errors.New("[ERROR]: cannot read from time windows directory")
	}

	for _, window := range windows {
		start, end, err := MinMaxTimestamp(window.Name())
		if err != nil {
			return err
		}

		if DoIntervalsOverlap(minTimestamp, maxTimestamp, start, end) {
			p, err := GetParquet(pm, windowsDir, window.Name(), ts, minTimestamp, maxTimestamp)
			if err != nil {
				return err
			}

			if p != "" {
				err = GetInParquet(pm, p, minTimestamp, maxTimestamp)
				if err != nil {
					return err
				}
			}
			continue
		}
		break
	}

	return nil
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

func GetInParquet(pm *page.Manager, parquetPath string, minTimestamp uint64, maxTimestamp uint64) error {
	rowGroups, err := os.ReadDir(parquetPath)
	if err != nil {
		return fmt.Errorf("[ERROR]: cannot read from parquet directory: %s", parquetPath)
	}

	for _, rg := range rowGroups {
		rgPath := filepath.Join(parquetPath, rg.Name())
		metaPath := filepath.Join(rgPath, "metadata.db")

		metaBytes, err := pm.ReadStructure(metaPath, 0)
		if err != nil {
			return err
		}

		meta, err := row_group.DeserializeMetadata(metaBytes)
		if err != nil {
			return err
		}

		if DoIntervalsOverlap(minTimestamp, maxTimestamp, meta.MinTimestamp, meta.MaxTimestamp) {
			err = GetInRowGroup(pm, rgPath, minTimestamp, maxTimestamp)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func DoIntervalsOverlap(min1, max1, min2, max2 uint64) bool {
	return min1 <= max2 && max1 >= min2
}

func GetInRowGroup(
	pm *page.Manager, rgPath string,
	minTimestamp uint64, maxTimestamp uint64,
) error {
	tsPath := filepath.Join(rgPath, "timestamp.db")
	valuePath := filepath.Join(rgPath, "value.db")
	deletePath := filepath.Join(rgPath, "delete.db")

	tsIter, err := NewIterator(pm, tsPath)
	if err != nil {
		return err
	}
	skipped, err := tsIter.Skip(minTimestamp, maxTimestamp)
	if err != nil {
		return err
	}

	valueIter, err := NewIterator(pm, valuePath)
	if err != nil {
		return err
	}
	err = valueIter.Advance(skipped)
	if err != nil {
		return err
	}

	deleteIter, err := NewIterator(pm, deletePath)
	if err != nil {
		return err
	}
	err = deleteIter.Advance(skipped)
	if err != nil {
		return err
	}

	for {
		e, err := tsIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		tsEntry := e.(*entry.TimestampEntry)
		if tsEntry.GetValue() > maxTimestamp {
			break
		}

		e, err = valueIter.Next()
		if err != nil {
			return err
		}
		valueEntry := e.(*entry.ValueEntry)

		e, err = deleteIter.Next()
		if err != nil {
			return err
		}
		deleteEntry := e.(*entry.DeleteEntry)

		fmt.Println(tsEntry, valueEntry, deleteEntry)
	}

	return nil
}
