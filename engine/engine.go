package engine

import (
	"bufio"
	"fmt"
	"os"
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
	configuration  *config.Config
	pageManager    *page_manager.Manager
	parquetManager *parquet.Manager
	memoryTable    *memory.MemTable
	wal            *write_ahead_log.WriteAheadLog
	timeWindow     *time_window.TimeWindow
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
	}

	e.timeWindow, err = e.checkTimeWindow()
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

	err = e.loadMemtables()
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (e *Engine) checkTimeWindow() (*time_window.TimeWindow, error) {
	now := uint64(time.Now().Unix())
	tw, err := time_window.LoadExistingTimeWindow(now, e.configuration.TimeWindowConfig.WindowsDirPath, &e.configuration.TimeWindowConfig, e.parquetManager)
	if err == nil {
		return tw, nil
	}

	// if there is no appropriate window
	tw, err = time_window.NewTimeWindow(now, e.configuration.TimeWindowConfig.WindowsDirPath, e.parquetManager, &e.configuration.TimeWindowConfig)
	if err != nil {
		return nil, err
	}
	e.parquetManager.Update(tw.Path)

	err = e.configuration.SetTimeWindowStart(now)
	if err != nil {
		return nil, err
	}

	return tw, nil
}

func (e *Engine) loadMemtables() error {
	offset, segmentIndex, pageIndex := e.prepareLoadMemtables()
	point := internal.Point{}

	for segmentIndex < e.wal.SegmentsNumber() {
		file, err := os.Stat(e.wal.SegmentFilename(segmentIndex))
		if err != nil {
			return err
		}

		err = e.reconstructWalSegment(uint64(file.Size()), offset, segmentIndex, pageIndex, &point)
		if err != nil {
			return err
		}

		offset = write_ahead_log.INDEX
		segmentIndex += 1
		pageIndex = 0
	}

	return nil
}

func (e *Engine) prepareLoadMemtables() (uint64, uint64, uint64) {
	offset := e.wal.UnstagedOffset()
	if offset == 0 {
		offset += write_ahead_log.INDEX
	}

	pageIndex := (offset - write_ahead_log.INDEX) / e.pageManager.Config.PageSize
	return offset, 0, pageIndex
}

func (e *Engine) reconstructWalSegment(
	fileSize uint64, offset uint64, segmentIndex uint64, pageIndex uint64, p *internal.Point) error {

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
				err = e.DeleteRange(timeSeries, walEntry.MinTimestamp, walEntry.MaxTimestamp)
				if err != nil {
					return err
				}
			} else {
				p.Value = walEntry.Value
				p.Timestamp = walEntry.MaxTimestamp

				_, err = e.putInMemtable(timeSeries, p, e.wal.ActiveSegment(), e.wal.UnstagedOffset())
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

	flushedPoints := e.memoryTable.WritePointWithFlush(ts, p)
	if flushedPoints != nil {
		deleteSegment = e.memoryTable.StartWALSegment
		e.memoryTable.StartWALSegment = walSegment
		e.memoryTable.StartWALOffset = walOffset

		err := e.timeWindow.FlushAll(flushedPoints)
		if err != nil {
			return "", err
		}
	}
	return deleteSegment, nil
}

func (e *Engine) Put(ts *internal.TimeSeries, p *internal.Point) error {
	walSeg := e.wal.ActiveSegment()
	walOff := e.wal.ActiveSegmentOffset()

	err := e.wal.Put(ts, p)
	if err != nil {
		return err
	}

	deleteSegment, err := e.putInMemtable(ts, p, walSeg, walOff)
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
	err := e.wal.Delete(ts, minTimestamp, maxTimestamp)
	if err != nil {
		return err
	}

	e.memoryTable.DeleteRange(ts, minTimestamp, maxTimestamp)

	// TODO: Delete on disk...
	return nil
}

func (e *Engine) List(ts *internal.TimeSeries, minTimestamp, maxTimestamp uint64) error {
	points := e.memoryTable.List(ts, minTimestamp, maxTimestamp)
	fmt.Println(points)

	err := disk.Get(
		e.pageManager,
		e.configuration.TimeWindowConfig.WindowsDirPath,
		ts,
		minTimestamp,
		maxTimestamp,
	)

	return err
}

func (e *Engine) Aggregate(
	ts *internal.TimeSeries,
	minTimestamp, maxTimestamp uint64,
	function string,
) error {
	switch function {
	case MIN:
		curBest, found := e.memoryTable.AggregateMinMax(ts, minTimestamp, maxTimestamp, true)
		if !found {
			fmt.Println("There are no points in given timestamp range")
			return nil
		}
		fmt.Printf("\nMinimum value is %f\n\n", curBest)
	case MAX:
		curBest, found := e.memoryTable.AggregateMinMax(ts, minTimestamp, maxTimestamp, false)
		if !found {
			fmt.Println("There are no points in given timestamp range")
			return nil
		}
		fmt.Printf("\nMaximum value is %f\n\n", curBest)
	default:
		return nil
	}

	return nil
}

func (e *Engine) Run() {
	// Helper functions for getting user input:
	getUserString := func(message string) string {
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
			return input
		}
	}
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
	getUserTags := func() internal.Tags {
		var numberOfTags uint64
		//for {
		//	numberOfTags = getUserInteger("Enter number of tags in time series:")
		//	if numberOfTags != 0 {
		//		break
		//	}
		//	fmt.Printf("\nEnter a postive integer!\n\n")
		//}
		tags := make(internal.Tags, 0)
		for i := 0; i < int(numberOfTags); i++ {
			name := getUserString("Enter tag name:")
			value := getUserString("Enter tag value:")
			tags = append(tags, internal.NewTag(name, value))
		}
		tags.Sort()
		return tags
	}
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
		fmt.Printf("Choose an option:\n\n")

		fmt.Println(" 1 - Write Point")
		fmt.Println(" 2 - Delete Range")
		fmt.Println(" 3 - List")
		fmt.Println(" 4 - Aggregate")

		fmt.Println("\n 0 - Exit")

		choice := getUserInteger("\nEnter your choice")
		switch choice {
		case 0:
			fmt.Println("Exiting the program.")
			return

		case 1:
			// Write Point functionality:
			//measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			//timestamp := getUserInteger("Enter point timestamp")
			value := getUserFloat("Enter point value")

			//err := e.Put(
			//	internal.NewTimeSeries(measurementName, tags),
			//	internal.NewPoint(value),
			//)
			err := e.Put(
				internal.NewTimeSeries("temp", tags),
				internal.NewPoint(value),
			)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 2:
			// Delete Range functionality:
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			err := e.DeleteRange(
				internal.NewTimeSeries(measurementName, tags),
				minTimestamp, maxTimestamp,
			)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 3:
			// List functionality:
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			err := e.List(
				internal.NewTimeSeries(measurementName, tags),
				minTimestamp, maxTimestamp,
			)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 4:
			// Aggregate functionality:
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			// Getting aggregation function:
			aggregationFunction := getUserAggregationFunc()

			err := e.Aggregate(
				internal.NewTimeSeries(measurementName, tags),
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
