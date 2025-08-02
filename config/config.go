package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type EngineConfig struct {
	RetentionPeriod int64  `yaml:"retention_period"`
	PeriodType      string `yaml:"period_type"`
}

type MemTableConfig struct {
	MaxSize uint64 `yaml:"max_size"`
}

type PageConfig struct {
	PageSize           uint64 `yaml:"max_size"`
	FilenameLength     uint64 `yaml:"filename_length"`
	BufferPoolCapacity uint64 `yaml:"buffer_pool_capacity"`
}

type ParquetConfig struct {
	PageSize     uint64 `yaml:"max_size"`
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

	// load system configuration
	sysConfigFile, err := os.Open("./config/sys_config.yaml")
	if err != nil {
		fmt.Println(err)
	}
	defer sysConfigFile.Close()

	var sysConfig Config
	decoder := yaml.NewDecoder(sysConfigFile)
	err = decoder.Decode(&sysConfig)
	if err != nil {
		fmt.Println(err)
	}

	// set default values if user messed up something
	// sysConfig.setDefaults()

	// save sys config on disk
	// sysConfig.Save("time-series-engine/config/sys_config.yaml")

	fmt.Println("Configuration loaded successfully.")

	return &sysConfig
}

func (c *Config) Save(filepath string) {
	file, err := os.OpenFile(filepath, os.O_WRONLY, 0644)
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
	// Memtable
	mc := &c.MemTableConfig
	if mc.MaxSize < 2 || mc.MaxSize > 10000 {
		mc.MaxSize = 1000
		fmt.Printf("Invalid Memtable max_size value. Set to default: %d\n", mc.MaxSize)
	}

	// TODO dodaj za page i parquet provjeru
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
