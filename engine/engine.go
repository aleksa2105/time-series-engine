package engine

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time-series-engine/config"
	"time-series-engine/internal"
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
	configuration config.Config
	memoryTable   memory.MemTable
	// TODO: Add WAL
}

func NewEngine() *Engine {
	conf := config.LoadConfiguration()
	memTable := *memory.NewMemTable(conf.MemTableConfig.MaxSize)

	return &Engine{
		configuration: conf,
		memoryTable:   memTable,
	}
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
			tags = append(tags, *internal.NewTag(name, value))
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
			fmt.Println(internal.NewTimeSeries(measurementName, tags).Hash())
			// TODO: add into Engine...
			// point := internal.NewPoint(timestamp, value)

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
