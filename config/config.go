package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

type EngineConfig struct {
	RetentionPeriod int64  `yaml:"retention_period"`
	PeriodType      string `yaml:"period_type"`
}

type MemTableConfig struct {
	MaxSize uint64 `yaml:"max_size"`
}

type PageConfig struct {
	PageSize           uint64 `yaml:"page_size"`
	FilenameLength     uint64 `yaml:"filename_length"`
	BufferPoolCapacity uint64 `yaml:"buffer_pool_capacity"`
}

type ParquetConfig struct {
	PageSize     uint64 `yaml:"page_size"`
	RowGroupSize uint64 `yaml:"row_group_size"`
}

type WALConfig struct {
	LogsDirPath        string `yaml:"logs_dir_path"`
	UnstagedOffset     uint64 `yaml:"unstaged_offset"`
	SegmentSizeInPages uint64 `yaml:"segment_size_in_pages"`
}

type TimeWindowConfig struct {
	Duration       uint64 `yaml:"duration"`
	Start          uint64 `yaml:"start"`
	WindowsDirPath string `yaml:"windows_dir_path"`
}

type Config struct {
	EngineConfig     `yaml:"engine"`
	MemTableConfig   `yaml:"memtable"`
	PageConfig       `yaml:"page"`
	ParquetConfig    `yaml:"parquet"`
	TimeWindowConfig `yaml:"time_window"`
	WALConfig        `yaml:"wal"`
}

func LoadConfiguration() *Config {
	fmt.Println("Loading configuration...")

	configFile, err := os.Open("./config/sys_config.yaml")
	if err != nil {
		fmt.Println(err)
	}
	defer configFile.Close()

	var sysConfig Config
	decoder := yaml.NewDecoder(configFile)
	err = decoder.Decode(&sysConfig)
	if err != nil {
		fmt.Println(err)
	}

	// set default values if user messed up something
	sysConfig.setDefaults()

	sysConfig.Save("./config/sys_config.yaml")

	fmt.Println("Configuration is loaded.")

	return &sysConfig
}

func (c *Config) Save(filepath string) {
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	err = encoder.Encode(c)
	if err != nil {
		fmt.Println(err)
	}
}

// setDefaults will fill empty and incorrect values with default ones
func (c *Config) setDefaults() {
	// MemTable
	mc := &c.MemTableConfig
	if mc.MaxSize < 2 || mc.MaxSize > 10000 {
		mc.MaxSize = 1000
		fmt.Printf("Invalid Memtable max_size value. Set to default: %d\n", mc.MaxSize)
	}

	// Engine
	ec := &c.EngineConfig
	if ec.RetentionPeriod < 1 || ec.RetentionPeriod > 60 {
		ec.RetentionPeriod = 2
		fmt.Printf("Invalid Engine retention_period value. Set to default: %d\n", ec.RetentionPeriod)
	}
	if ec.PeriodType != "minute" && ec.PeriodType != "hour" && ec.PeriodType != "day" {
		ec.PeriodType = "minute"
		fmt.Printf("Invalid Engine period_type value. Set to default: %s\n", ec.PeriodType)
	}

	// Page
	pc := &c.PageConfig
	if pc.PageSize < 256 || pc.PageSize > 16000 {
		pc.PageSize = 1000
		fmt.Printf("Invalid Page page_size value. Set to default: %d\n", pc.PageSize)
	}
	if pc.FilenameLength < 1 || pc.FilenameLength > 10 {
		pc.FilenameLength = 4
		fmt.Printf("Invalid Page filename_length value. Set to default: %d\n", pc.FilenameLength)
	}
	if pc.BufferPoolCapacity < 1 || pc.BufferPoolCapacity > 10_000 {
		pc.BufferPoolCapacity = 100
		fmt.Printf("Invalid Page buffer_pool_capacity value. Set to default: %d\n", pc.BufferPoolCapacity)
	}

	// Parquet
	pq := &c.ParquetConfig
	if pq.PageSize < 256 || pq.PageSize > 16000 {
		pq.PageSize = 1000
		fmt.Printf("Invalid Parquet page_size value. Set to default: %d\n", pq.PageSize)
	}
	if pq.RowGroupSize < 1 || pq.RowGroupSize > 100 {
		pq.RowGroupSize = 3
		fmt.Printf("Invalid Parquet row_group_size value. Set to default: %d\n", pq.RowGroupSize)
	}

	// Time Window
	tw := &c.TimeWindowConfig
	if tw.Duration < 1 || tw.Duration > 86400 {
		tw.Duration = 90
		fmt.Printf("Invalid TimeWindow duration value. Set to default: %d\n", tw.Duration)
	}
	if strings.TrimSpace(tw.WindowsDirPath) == "" {
		tw.WindowsDirPath = "./db/data"
		fmt.Printf("Empty TimeWindow windows_dir_path. Set to default: %s\n", tw.WindowsDirPath)
	}

	// WAL
	w := &c.WALConfig
	if strings.TrimSpace(w.LogsDirPath) == "" {
		w.LogsDirPath = "./db/logs"
		fmt.Printf("Empty WAL logs_dir_path. Set to default: %s\n", w.LogsDirPath)
	}
	if w.SegmentSizeInPages < 1 || w.SegmentSizeInPages > 512 {
		w.SegmentSizeInPages = 2
		fmt.Printf("Invalid WAL segment_size_in_pages value. Set to default: %d\n", w.SegmentSizeInPages)
	}
}

func (c *Config) SetUnstagedOffset(offset uint64) error {
	c.WALConfig.UnstagedOffset = offset

	updatedFile, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to encode updated YAML: %w", err)
	}

	if err := os.WriteFile("./config/sys_config.yaml", updatedFile, 0644); err != nil {
		return fmt.Errorf("failed to write updated config file: %w", err)
	}

	return nil
}

func (c *Config) SetTimeWindowStart(start uint64) error {
	c.TimeWindowConfig.Start = start

	updatedFile, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to encode updated YAML: %w", err)
	}

	if err := os.WriteFile("./config/sys_config.yaml", updatedFile, 0644); err != nil {
		return fmt.Errorf("failed to write updated config file: %w", err)
	}

	return nil
}
