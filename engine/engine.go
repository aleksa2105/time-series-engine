package engine

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/page/page_manager"
	"time-series-engine/internal/disk/parquet"
	"time-series-engine/internal/disk/row_group"
	"time-series-engine/internal/disk/time_window"
	"time-series-engine/internal/disk/write_ahead_log"
	"time-series-engine/internal/memory"
)

// Aggregation functions:
const (
	MIN  = "Min"
	MAX  = "Max"
	MEAN = "Mean"
	AVG  = "Average"
)

func GetAllAggregationFunctions() []string {
	return []string{MIN, MAX, MEAN, AVG}
}

// Reader for user input. I had problems with \n while using fmt.Scanln():
var reader = bufio.NewReader(os.Stdin)

type Engine struct {
	configuration   *config.Config
	pageManager     *page_manager.Manager
	parquetManager  *parquet.Manager
	memoryTable     *memory.MemTable
	wal             *write_ahead_log.WriteAheadLog
	timeWindow      *time_window.TimeWindow
	recovering      bool
	retentionPeriod int64
}

func NewEngine() (*Engine, error) {
	var err error
	conf := config.LoadConfiguration()
	pm := page_manager.NewManager(conf.PageConfig)
	wal := write_ahead_log.NewWriteAheadLog(&conf.WALConfig, pm)
	memTable := memory.NewMemTable(conf.MemTableConfig.MaxSize)
	parquetManager := parquet.NewManager(&conf.ParquetConfig, pm, "")

	e := Engine{
		configuration:  conf,
		pageManager:    pm,
		memoryTable:    memTable,
		wal:            wal,
		parquetManager: parquetManager,
		recovering:     true,
	}

	err = e.loadTimeWindow()
	if err != nil {
		return nil, err
	}
	switch e.configuration.PeriodType {
	case "minute":
		e.retentionPeriod = 60 * e.configuration.RetentionPeriod
		break
	case "hour":
		e.retentionPeriod = 60 * 60 * e.configuration.RetentionPeriod
		break
	case "day":
		e.retentionPeriod = 60 * 60 * 24 * e.configuration.RetentionPeriod
		break
	}
	err = e.checkRetentionPeriod()
	if err != nil {
		return nil, err
	}

	err = e.wal.LoadWal()
	if err != nil {
		return nil, err
	}

	err = e.configuration.SetUnstagedOffset(wal.UnstagedOffset())
	if err != nil {
		return nil, err
	}

	err = e.loadMemtable()
	if err != nil {
		return nil, err
	}

	e.recovering = false
	return &e, nil
}

func (e *Engine) checkRetentionPeriod() error {
	path := e.configuration.TimeWindowConfig.WindowsDirPath
	files, err := os.ReadDir(path)
	retention := time.Now().Unix() - e.retentionPeriod
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			_, maxTimestamp, err := disk.MinMaxTimestamp(f.Name())
			if err != nil {
				return err
			}
			if maxTimestamp <= uint64(retention) {
				err = e.pageManager.RemoveFile(filepath.Join(path, f.Name()))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// loadTimeWindow loads already existing time window, or creates new one instead
func (e *Engine) loadTimeWindow() error {
	now := uint64(time.Now().Unix())
	tw, err := time_window.LoadExistingTimeWindow(now, e.configuration.TimeWindowConfig.WindowsDirPath, &e.configuration.TimeWindowConfig, e.parquetManager)

	if tw != nil && err == nil {
		e.timeWindow = tw
		return nil
	}

	// if there is no appropriate window
	tw, err = time_window.NewTimeWindow(now, e.configuration.TimeWindowConfig.WindowsDirPath, e.parquetManager, &e.configuration.TimeWindowConfig)
	if err != nil {
		return err
	}
	e.timeWindow = tw
	e.parquetManager.Update(tw.Path)

	err = e.configuration.TimeWindowConfig.SetTimeWindowStart(now)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) loadMemtable() error {
	offset, segmentIndex, pageIndex := e.prepareLoadMemtable()

	e.memoryTable.StartWALOffset = offset
	e.memoryTable.StartWALSegment = e.wal.FirstSegment()

	for segmentIndex < e.wal.SegmentsNumber() {
		file, err := os.Stat(e.wal.SegmentFilename(segmentIndex))
		if err != nil {
			return err
		}

		err = e.reconstructWalSegment(uint64(file.Size()), offset, segmentIndex, pageIndex)
		if err != nil {
			return err
		}

		offset = write_ahead_log.INDEX
		segmentIndex += 1
		pageIndex = 0
	}

	return nil
}

func (e *Engine) prepareLoadMemtable() (uint64, uint64, uint64) {
	offset := e.wal.UnstagedOffset()
	if offset == 0 {
		offset += write_ahead_log.INDEX
	}

	pageIndex := (offset - write_ahead_log.INDEX) / e.pageManager.Config.PageSize
	return offset, 0, pageIndex
}

func (e *Engine) reconstructWalSegment(
	fileSize uint64, offset uint64, segmentIndex uint64, pageIndex uint64) error {

	currentOffset := write_ahead_log.INDEX + pageIndex*e.pageManager.Config.PageSize
	for offset < fileSize {
		pageBytes, err := e.pageManager.ReadPage(
			e.wal.SegmentFilename(segmentIndex),
			write_ahead_log.INDEX+int64(pageIndex*e.pageManager.Config.PageSize))
		if err != nil {
			return err
		}

		walPage, err := page.DeserializeWALPage(pageBytes)
		if err != nil {
			return err
		}
		for _, en := range walPage.GetEntries() {
			walEntry := en.(*entry.WALEntry)
			if currentOffset < offset {
				currentOffset += walEntry.Size()
				continue
			}

			timeSeries := internal.NewTimeSeries(walEntry.MeasurementName, walEntry.Tags)
			if walEntry.Delete {
				e.memoryTable.DeleteRange(timeSeries, walEntry.MinTimestamp, walEntry.MaxTimestamp)
			} else {
				newPoint := &internal.Point{
					Value:     walEntry.Value,
					Timestamp: walEntry.MaxTimestamp,
				}
				_, err = e.putInMemtable(timeSeries, newPoint, e.wal.ActiveSegment(), e.wal.UnstagedOffset())
				if err != nil {
					return err
				}
			}

			entrySize := walEntry.Size()
			offset += entrySize
			currentOffset += entrySize
		}

		pageIndex += 1
		offset = write_ahead_log.INDEX + pageIndex*e.pageManager.Config.PageSize
		currentOffset = offset
	}

	return nil
}

func (e *Engine) putInMemtable(ts *internal.TimeSeries, p *internal.Point, walSegment string, walOffset uint64) (string, error) {
	var deleteSegment string
	if !e.recovering {
		err := e.timeWindow.Update(p.Timestamp)
		if err != nil {
			return "", err
		}
	} else {
		if p.Timestamp < e.timeWindow.StartTimestamp || p.Timestamp >= e.timeWindow.EndTimestamp {
			return "", nil
		}
	}
	flushedPoints := e.memoryTable.WritePointWithFlush(ts, p)
	if flushedPoints != nil {
		deleteSegment = e.memoryTable.StartWALSegment
		e.memoryTable.StartWALSegment = walSegment
		e.memoryTable.StartWALOffset = walOffset

		err := e.configuration.SetUnstagedOffset(walOffset)
		if err != nil {
			return "", err
		}

		groups, err := e.prepareFlush(flushedPoints)
		if err != nil {
			return "", err
		}

		err = e.flush(groups)
		if err != nil {
			return "", err
		}
	}
	return deleteSegment, nil
}

func (e *Engine) prepareFlush(flushedPoints map[string][]*internal.Point) (map[string]map[string][]*internal.Point, error) {
	windowGroups := make(map[string]map[string][]*internal.Point)

	for tsName, points := range flushedPoints {
		currentTw := e.timeWindow
		for _, point := range points {
			if !e.timeWindow.Belongs(point.Timestamp) {
				if !currentTw.Belongs(point.Timestamp) {
					tw, err := time_window.LoadExistingTimeWindow(point.Timestamp, e.configuration.WindowsDirPath, &e.configuration.TimeWindowConfig, e.parquetManager)
					if err != nil {
						return nil, err
					}
					currentTw = tw
				}

				windowID := currentTw.Path
				if _, ok := windowGroups[windowID]; !ok {
					windowGroups[windowID] = make(map[string][]*internal.Point)
				}
				windowGroups[windowID][tsName] = append(windowGroups[windowID][tsName], point)
			} else {
				if _, ok := windowGroups[e.timeWindow.Path]; !ok {
					windowGroups[e.timeWindow.Path] = make(map[string][]*internal.Point)
				}
				windowGroups[e.timeWindow.Path][tsName] = append(windowGroups[e.timeWindow.Path][tsName], point)
			}
		}
	}
	return windowGroups, nil
}

func (e *Engine) flush(windowGroups map[string]map[string][]*internal.Point) error {
	currentTw := e.timeWindow
	var err error
	for winID, group := range windowGroups {
		if winID != currentTw.Path {
			var examplePoint *internal.Point
			found := false
			for _, points := range group {
				if len(points) > 0 {
					examplePoint = points[0]
					found = true
					break
				}
			}
			if !found {
				continue
			}
			currentTw, err = time_window.LoadExistingTimeWindow(examplePoint.Timestamp, e.configuration.WindowsDirPath, &e.configuration.TimeWindowConfig, e.parquetManager)
			if err != nil {
				return err
			}
			err = currentTw.FlushAll(group)
			if err != nil {
				return err
			}
		} else {
			err = currentTw.FlushAll(group)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) Put(ts *internal.TimeSeries, p *internal.Point) error {
	err := e.checkRetentionPeriod()
	if err != nil {
		fmt.Printf("\n[ERROR]: %v\n\n", err)
	}

	walSeg := e.wal.ActiveSegment()
	offset, err := e.wal.Put(ts, p)
	if err != nil {
		return err
	}

	deleteSegment, err := e.putInMemtable(ts, p, walSeg, offset)
	if err != nil {
		return err
	}

	_, err = e.wal.DeleteWalSegments(deleteSegment)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) DeleteRange(ts *internal.TimeSeries, minTimestamp, maxTimestamp uint64) error {
	err := e.checkRetentionPeriod()
	if err != nil {
		fmt.Printf("\n[ERROR]: %v\n\n", err)
	}

	err = e.wal.Delete(ts, minTimestamp, maxTimestamp)
	if err != nil {
		return err
	}

	e.memoryTable.DeleteRange(ts, minTimestamp, maxTimestamp)

	err = e.DeleteInParquet(ts, minTimestamp, maxTimestamp)
	if err != nil {
		return err
	}
	return nil
}

func (e *Engine) DeleteInParquet(ts *internal.TimeSeries, minTimestamp uint64, maxTimestamp uint64) error {
	windowsDir := e.configuration.WindowsDirPath
	windows, err := os.ReadDir(windowsDir)

	if err != nil {
		return err
	}

	for _, window := range windows {
		start, end, err := disk.MinMaxTimestamp(window.Name())
		if err != nil {
			return err
		}

		if disk.DoIntervalsOverlap(minTimestamp, maxTimestamp, start, end) {
			p, err := disk.GetParquet(e.pageManager, windowsDir, window.Name(), ts, minTimestamp, maxTimestamp)
			if err != nil {
				return err
			}

			if p != "" {
				err = e.DeleteInRowGroup(p, minTimestamp, maxTimestamp)
				if err != nil {
					return err
				}
			}
			continue
		}
		continue
	}
	return nil
}

func (e *Engine) DeleteInRowGroup(parquetPath string, minTimestamp uint64, maxTimestamp uint64) error {
	rowGroups, err := os.ReadDir(parquetPath)
	if err != nil {
		return err
	}

	for _, rg := range rowGroups {
		if !rg.IsDir() {
			continue
		}

		rgPath := filepath.Join(parquetPath, rg.Name())
		metaPath := filepath.Join(rgPath, "metadata.db")

		metaBytes, err := e.pageManager.ReadStructure(metaPath, 0)
		if err != nil {
			return err
		}

		meta, err := row_group.DeserializeMetadata(metaBytes)
		if err != nil {
			return err
		}

		if disk.DoIntervalsOverlap(minTimestamp, maxTimestamp, meta.MinTimestamp, meta.MaxTimestamp) {
			tsPath := filepath.Join(rgPath, "timestamp.db")
			deletePath := filepath.Join(rgPath, "delete.db")

			tsIter, err := disk.NewIterator(e.pageManager, tsPath, disk.Timestamp)
			if err != nil {
				return err
			}
			skipped, err := tsIter.Skip(minTimestamp, maxTimestamp)
			if err != nil {
				return err
			}

			deleteIter, err := disk.NewIterator(e.pageManager, deletePath, disk.Delete)
			if err != nil {
				return err
			}
			err = deleteIter.Advance(skipped)
			if err != nil {
				return err
			}

			for {
				en, err := tsIter.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				tsEntry := en.(*entry.TimestampEntry)
				if tsEntry.GetValue() > maxTimestamp {
					break
				}
				if !deleteIter.HasNext() {
					err = e.pageManager.WritePage(deleteIter.ActivePage, deletePath, int64(deleteIter.CurrentPageOffset-e.configuration.PageConfig.PageSize))
					if err != nil {
						return err
					}
				}
				en, err = deleteIter.Next()
				if err != nil {
					return err
				}
				deleteEntry := en.(*entry.DeleteEntry)
				deleteEntry.Delete()
			}
			err = e.pageManager.WritePage(deleteIter.ActivePage, deletePath, int64(deleteIter.CurrentPageOffset-e.configuration.PageConfig.PageSize))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) List(ts *internal.TimeSeries, minTimestamp, maxTimestamp uint64) error {
	err := e.checkRetentionPeriod()
	if err != nil {
		fmt.Printf("\n[ERROR]: %v\n\n", err)
	}

	pointsMemory := e.memoryTable.List(ts, minTimestamp, maxTimestamp)

	err = e.checkRetentionPeriod()
	if err != nil {
		fmt.Printf("\n[ERROR]: %v\n\n", err)
	}
	pointsDisk, err := disk.Get(
		e.pageManager,
		e.configuration.TimeWindowConfig.WindowsDirPath,
		ts,
		minTimestamp,
		maxTimestamp,
	)

	fmt.Println()
	for _, p := range pointsDisk {
		fmt.Println(p)
	}
	for _, p := range pointsMemory {
		fmt.Println(p)
	}
	fmt.Println()
	return err
}

func (e *Engine) Aggregate(
	ts *internal.TimeSeries,
	minTimestamp, maxTimestamp uint64,
	function string,
) error {
	err := e.checkRetentionPeriod()
	if err != nil {
		fmt.Printf("\n[ERROR]: %v\n\n", err)
	}
	switch function {
	case MIN:
		curBest, _, found := e.memoryTable.Aggregate(ts, minTimestamp, maxTimestamp, MIN)
		if !found {
			curBest = math.MaxFloat64
		}
		diskBest, _, err := disk.Aggregate(ts, minTimestamp, maxTimestamp, e.pageManager, e.configuration.WindowsDirPath, MIN)
		if err != nil {
			return err
		}
		if diskBest < curBest {
			curBest = diskBest
		}
		if curBest != math.MaxFloat64 {
			fmt.Printf("\nMinimum value is %.2f\n\n", curBest)
		}
	case MAX:
		curBest, _, found := e.memoryTable.Aggregate(ts, minTimestamp, maxTimestamp, MAX)
		if !found {
			curBest = -math.MaxFloat64
		}
		diskBest, _, err := disk.Aggregate(ts, minTimestamp, maxTimestamp, e.pageManager, e.configuration.WindowsDirPath, MAX)
		if err != nil {
			return err
		}
		if diskBest > curBest {
			curBest = diskBest
		}
		if curBest != -math.MaxFloat64 {
			fmt.Printf("\nMaximum value is %.2f\n\n", curBest)
		}
	case AVG:
		memorySum, memoryEntriesNum, found := e.memoryTable.Aggregate(ts, minTimestamp, maxTimestamp, AVG)
		if !found {
			memorySum = 0
			memoryEntriesNum = 0
		}
		var totalCount uint64
		var result float64
		diskSum, diskEntriesNum, err := disk.Aggregate(ts, minTimestamp, maxTimestamp, e.pageManager, e.configuration.WindowsDirPath, AVG)
		if err != nil {
			return err
		}
		totalCount = diskEntriesNum + memoryEntriesNum
		if totalCount == 0 {
			result = 0
		} else {
			result = (memorySum + diskSum) / float64(totalCount)
		}
		fmt.Printf("\nAverage value is %.2f\n\n", result)
	default:
		return nil
	}

	return nil
}

func (e *Engine) Run() {
	// Helper functions for getting user input:
	//getUserString := func(message string) string {
	//	for {
	//		fmt.Printf("%s ", message)
	//		input, err := reader.ReadString('\n')
	//		if err != nil {
	//			fmt.Printf("\n[ERROR]: %v\n\n", err)
	//			continue
	//		}
	//		input = strings.TrimSpace(input)
	//		if input == "" {
	//			fmt.Printf("\nEnter something!\n\n")
	//			continue
	//		}
	//		return input
	//	}
	//}
	getUserInteger := func(message string) uint64 {
		for {
			fmt.Printf("%s ", message)
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
				continue
			}
			input = strings.TrimSpace(input)
			if input == "" {
				fmt.Printf("\nEnter something!\n\n")
				continue
			}
			parsedNumber, err := strconv.ParseUint(input, 10, 64)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
				continue
			}
			return parsedNumber
		}
	}
	getUserFloat := func(message string) float64 {
		for {
			fmt.Printf("%s ", message)
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
				continue
			}
			input = strings.TrimSpace(input)
			if input == "" {
				fmt.Printf("\nEnter something!\n\n")
				continue
			}
			parsedFloat, err := strconv.ParseFloat(input, 64)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
				continue
			}
			return parsedFloat
		}
	}
	//getUserTags := func() internal.Tags {
	//	var numberOfTags uint64
	//	for {
	//		numberOfTags = getUserInteger("Enter number of tags in time series:")
	//		if numberOfTags != 0 {
	//			break
	//		}
	//		fmt.Printf("\nEnter a postive integer!\n\n")
	//	}
	//	tags := make(internal.Tags, 0)
	//	for i := 0; i < int(numberOfTags); i++ {
	//		name := getUserString("Enter tag name:")
	//		value := getUserString("Enter tag value:")
	//		tags = append(tags, internal.NewTag(name, value))
	//	}
	//	tags.Sort()
	//	return tags
	//}
	getUserMinMaxTimestamp := func() (uint64, uint64) {
		minTimestamp := getUserInteger("Enter minimum timestamp:")
		for {
			maxTimestamp := getUserInteger("Enter maximum timestamp:")
			if maxTimestamp >= minTimestamp {
				return minTimestamp, maxTimestamp
			}
			fmt.Printf("\nMaximum timestamp can't be smaller than minimum!\n\n")
		}
	}
	getUserAggregationFunc := func() string {
		aggFunctions := GetAllAggregationFunctions()
		maxIndex := uint64(len(aggFunctions))
		fmt.Printf("Select aggregation function:\n\n")
		for i, function := range aggFunctions {
			fmt.Printf(" %d - %s\n", i+1, function)
		}
		fmt.Println()
		for {
			userInput := getUserInteger(">>")
			if 1 <= userInput && userInput <= maxIndex {
				return aggFunctions[userInput-1]
			}
			fmt.Printf("\nYou must select a number from range [%d, %d]!\n\n", 1, maxIndex)
		}
	}

	// Main loop:
	for {
		fmt.Println()
		fmt.Println(" 1 - Write Point")
		fmt.Println(" 2 - Delete Range")
		fmt.Println(" 3 - List")
		fmt.Println(" 4 - Aggregate")
		fmt.Println("\n 0 - Exit")

		choice := getUserInteger("\nEnter your choice: ")
		switch choice {
		case 0:
			fmt.Println("Exiting the program.")
			return

		case 1:
			// Write Point functionality:
			//measurementName := getUserString("Enter time series measurement name")
			//tags := getUserTags()
			////timestamp := getUserInteger("Enter point timestamp")
			value := getUserFloat("Enter point value: ")

			err := e.Put(
				//internal.NewTimeSeries(measurementName, tags),
				internal.NewTimeSeries("temp", nil),
				internal.NewPoint(value),
			)

			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 2:
			// Delete Range functionality:
			//measurementName := getUserString("Enter time series measurement name: ")
			//tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			err := e.DeleteRange(
				//internal.NewTimeSeries(measurementName, tags),
				internal.NewTimeSeries("temp", nil),
				minTimestamp, maxTimestamp,
			)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 3:
			// List functionality:
			//measurementName := getUserString("Enter time series measurement name: ")
			//tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			err := e.List(
				//internal.NewTimeSeries(measurementName, tags),
				internal.NewTimeSeries("temp", nil),
				minTimestamp, maxTimestamp,
			)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 4:
			// Aggregate functionality:
			//measurementName := getUserString("Enter time series measurement name: ")
			//tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			// Getting aggregation function:
			aggregationFunction := getUserAggregationFunc()

			err := e.Aggregate(
				//internal.NewTimeSeries(measurementName, tags),
				internal.NewTimeSeries("temp", nil),
				minTimestamp, maxTimestamp,
				aggregationFunction,
			)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		default:
			fmt.Printf("\nInvalid choice, please try again!\n\n")
		}
	}
}
