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
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/parquet"
	"time-series-engine/internal/disk/time_window"
	"time-series-engine/internal/disk/write_ahead_log"
	"time-series-engine/internal/memory"
)

type AggregationFunc string

const (
	MIN  AggregationFunc = "Min"
	MAX  AggregationFunc = "Max"
	MEAN AggregationFunc = "Mean"
	AVG  AggregationFunc = "Average"
)

func GetAllAggregationFunctions() []AggregationFunc {
	return []AggregationFunc{MIN, MAX, MEAN, AVG}
}

// Reader for user input. I had problems with \n while using fmt.Scanln():
var reader = bufio.NewReader(os.Stdin)

type Engine struct {
	configuration  *config.Config
	pageManager    *page.Manager
	parquetManager *parquet.Manager
	memoryTable    *memory.MemTable
	wal            *write_ahead_log.WriteAheadLog
	timeWindow     *time_window.TimeWindow
}

func NewEngine() (*Engine, error) {
	var err error
	conf := config.LoadConfiguration()
	pm := page.NewManager(conf.PageConfig)
	wal := write_ahead_log.NewWriteAheadLog(&conf.WALConfig, pm)
	memTable := memory.NewMemTable(conf.MemTableConfig.MaxSize)

	e := Engine{
		configuration: conf,
		pageManager:   pm,
		memoryTable:   memTable,
		wal:           wal,
	}

	e.timeWindow, err = e.checkTimeWindow()
	if err != nil {
		return nil, err
	}

	e.parquetManager = parquet.NewManager(&conf.ParquetConfig, pm, fmt.Sprintf("time_window_%s", conf.TimeWindowConfig.Start))

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
	var tw *time_window.TimeWindow
	var err error
	if e.configuration.TimeWindowConfig.Start == 0 || time.Unix(e.configuration.TimeWindowConfig.Start, 0).Before(time.Now()) {
		newStart := time.Now().Unix()
		tw, err = time_window.NewTimeWindow(uint64(newStart), fmt.Sprintf("time_window_%s", newStart), e.parquetManager, &e.configuration.TimeWindowConfig)
		if err != nil {
			return nil, err
		}
		err = e.configuration.SetTimeWindowStart(newStart)
		if err != nil {
			return nil, err
		}
	} else {
		tw, err = time_window.NewTimeWindow(uint64(e.configuration.TimeWindowConfig.Start), fmt.Sprintf("time_window_%s", e.configuration.TimeWindowConfig.Start), e.parquetManager, &e.configuration.TimeWindowConfig)
		if err != nil {
			return nil, err
		}
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
		for _, en := range walPage.Entries {
			if currentOffset < offset {
				currentOffset += en.Size()
				continue
			}
			p.Value = en.Value
			p.Timestamp = en.Timestamp
			timeSeries := internal.NewTimeSeries(en.MeasurementName, en.Tags)

			_, err := e.putInMemtable(timeSeries, p, e.wal.ActiveSegment(), e.wal.UnstagedOffset())
			if err != nil {
				return err
			}

			entrySize := en.Size()
			offset += entrySize
			currentOffset += entrySize
		}

		pageIndex += 1
		offset = write_ahead_log.INDEX + pageIndex*e.pageManager.Config.PageSize
		currentOffset = offset
	}

	return nil
}

func (e *Engine) putInMemtable(ts *internal.TimeSeries, p *internal.Point, wallSegment string, wallOffset uint64) (uint64, error) {
	var deletedSegmentsNumber uint64 = 0

	flushedPoints := e.memoryTable.WritePointWithFlush(ts, p)
	if flushedPoints != nil {
		// TODO
	}
	return deletedSegmentsNumber, nil
}

func (e *Engine) Put(ts *internal.TimeSeries, p *internal.Point) error {
	walSeg := e.wal.ActiveSegment()
	walOff := e.wal.ActiveSegmentOffset()

	err := e.wal.Put(ts, p)
	if err != nil {
		return err
	}

	_, err = e.putInMemtable(ts, p, walSeg, walOff)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) List(ts *internal.TimeSeries) *memory.DoublyLinkedList {
	return e.memoryTable.Data[ts.Hash()]
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
		for {
			numberOfTags = getUserInteger("Enter number of tags in time series:")
			if numberOfTags != 0 {
				break
			}
			fmt.Printf("\nEnter a postive integer!\n\n")
		}
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
	getUserAggregationFunc := func() AggregationFunc {
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
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			timestamp := getUserInteger("Enter point timestamp")
			value := getUserFloat("Enter point value")

			fmt.Printf("Measurement name: %v, Tags: %v, Timestamp: %d, Value: %f\n",
				measurementName, tags, timestamp, value)

			ts := internal.NewTimeSeries(measurementName, tags)
			point := internal.NewPoint(timestamp, value)
			err := e.Put(ts, &point)
			if err != nil {
				fmt.Printf("\n[ERROR]: %v\n\n", err)
			}

		case 2:
			// Delete Range functionality:
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			fmt.Printf("Measurement name: %v, Tags: %v, Min Timestamp: %d, Max Timestamp: %d\n",
				measurementName, tags, minTimestamp, maxTimestamp)
			// TODO: modify Engine...

		case 3:
			// List functionality:
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			fmt.Printf("Measurement name: %v, Tags: %v, Min Timestamp: %d, Max Timestamp: %d\n",
				measurementName, tags, minTimestamp, maxTimestamp)
			// TODO: modify Engine...
			ts := internal.NewTimeSeries(measurementName, tags)
			for _, in := range e.List(ts).GetSortedPoints() {
				fmt.Println(in)
			}

		case 4:
			// Aggregate functionality:
			measurementName := getUserString("Enter time series measurement name")
			tags := getUserTags()
			minTimestamp, maxTimestamp := getUserMinMaxTimestamp()

			// Getting aggregation function:
			aggregationFunction := getUserAggregationFunc()
			fmt.Printf(
				"Measurement name: %v, Tags: %v, Min Timestamp: %d, Max Timestamp: %d, Function: %s\n",
				measurementName, tags, minTimestamp, maxTimestamp, aggregationFunction)
			// TODO: modify Engine...

		default:
			fmt.Printf("\nInvalid choice, please try again!\n\n")
		}
	}
}
