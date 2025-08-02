package time_window

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/parquet"
)

type TimeWindow struct {
	StartTimestamp uint64
	EndTimestamp   uint64
	WindowsDir     string
	Path           string
	ParquetManager *parquet.Manager
	Config         *config.TimeWindowConfig
}

func NewTimeWindow(startTimestamp uint64, windowsDir string,
	parquetManager *parquet.Manager, c *config.TimeWindowConfig) (*TimeWindow, error) {
	tw := &TimeWindow{
		StartTimestamp: startTimestamp,
		EndTimestamp:   startTimestamp + c.Duration,
		WindowsDir:     windowsDir,
		ParquetManager: parquetManager,
		Config:         c,
	}

	err := tw.CreateNewWindowDirectory()
	if err != nil {
		return nil, err
	}

	return tw, nil
}

func (tw *TimeWindow) Belongs(timestamp uint64) bool {
	if tw.StartTimestamp <= timestamp && timestamp <= tw.EndTimestamp {
		return true
	}
	return false
}

func (tw *TimeWindow) CreateNewWindowDirectory() error {
	newFolderName := fmt.Sprintf("window_%d-%d", tw.StartTimestamp, tw.EndTimestamp)
	newPath := filepath.Join(tw.WindowsDir, newFolderName)

	err := os.MkdirAll(newPath, 0755)
	if err != nil {
		return fmt.Errorf("failed to create new time window directory: %w", err)
	}

	tw.Path = newPath
	return nil
}

func (tw *TimeWindow) FlushAll(series map[string][]*internal.Point) error {
	return tw.ParquetManager.FlushAll(series)
}

func (tw *TimeWindow) FlushSeries(timeSeriesHash string, points []*internal.Point) error {
	return tw.ParquetManager.FlushSeries(timeSeriesHash, points)
}

func LoadExistingTimeWindow(currentTime uint64, windowsDir string, conf *config.TimeWindowConfig, parquetManager *parquet.Manager) (*TimeWindow, error) {
	files, err := os.ReadDir(windowsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read windows directory: %w", err)
	}

	re := regexp.MustCompile(`^window_(\d+)-(\d+)$`)

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		match := re.FindStringSubmatch(f.Name())
		if len(match) != 3 {
			continue
		}

		start, err1 := strconv.ParseUint(match[1], 10, 64)
		end, err2 := strconv.ParseUint(match[2], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}

		if currentTime >= start && currentTime < end {
			tw := &TimeWindow{
				StartTimestamp: start,
				EndTimestamp:   end,
				WindowsDir:     windowsDir,
				Path:           filepath.Join(windowsDir, f.Name()),
				ParquetManager: parquetManager,
				Config:         conf,
			}
			tw.ParquetManager.Update(tw.Path)
			return tw, nil
		}
	}

	return nil, fmt.Errorf("no time window matches current time")
}
