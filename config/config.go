package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type MemTableConfig struct {
	NumOfInstances uint64 `yaml:"num_of_instances"`
	MaxSize        uint64 `yaml:"max_size"`
}

type TBConfig struct {
	User     string        `yaml:"user"`
	Capacity uint64        `yaml:"capacity"`
	Interval time.Duration `yaml:"interval"`
}

type PageConfig struct {
	PageSize       uint64 `yaml:"max_size"`
	FilenameLength uint64 `yaml:"file_name_length"`
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

type Config struct {
	MemTableConfig `yaml:"memtable"`
	PageConfig     `yaml:"page"`
	TBConfig       `yaml:"token_bucket"`
	ParquetConfig  `yaml:"parquet"`
	WALConfig      `yaml:"wal"`
}

func LoadConfiguration() Config {
	fmt.Println("Loading configuration...")

	// load system configuration
	sysConfigFile, err := os.Open("./config/sys_config.yaml")
	if err != nil {
		fmt.Println(err)
	}
	defer sysConfigFile.Close()

	var sysConfig Config
	decoder := yaml.NewDecoder(sysConfigFile)
	if err := decoder.Decode(&sysConfig); err != nil {
		fmt.Println(err)
	}

	// load user configuration
	userConfigFile, err := os.Open("./config/user_config.yaml")
	if err != nil {
		fmt.Println(err)
	}
	defer userConfigFile.Close()

	var userConfig Config
	decoder = yaml.NewDecoder(userConfigFile)
	if err := decoder.Decode(&userConfig); err != nil {
		fmt.Println(err)
	}

	// set default values if user messed up something
	sysConfig.setDefaults()

	// save sys config on disk
	sysConfig.Save("./config/sys_config.yaml")

	fmt.Println("Configuration loaded successfully.")

	return sysConfig
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
	if mc.NumOfInstances < 1 || mc.NumOfInstances > 4 {
		mc.NumOfInstances = 3
		fmt.Printf("Invalid Memtable num_of_instances value. Set to default: %d\n", mc.NumOfInstances)
	}
	if mc.MaxSize < 2 || mc.MaxSize > 10000 {
		mc.MaxSize = 1000
		fmt.Printf("Invalid Memtable max_size value. Set to default: %d\n", mc.MaxSize)
	}

	// TODO dodaj za page i parquet provjeru

	// Token Bucket
	tb := &c.TBConfig
	if tb.User == "" {
		tb.User = "user"
		fmt.Println("Invalid TokenBucket user value. Set to default:", tb.User)
	}
	if tb.Capacity < 1 || tb.Capacity > 1000 {
		tb.Capacity = 100
		fmt.Println("Invalid TokenBucket capacity value. Set to default:", tb.Capacity)
	}
	if tb.Interval < time.Second || tb.Interval > time.Minute*5 {
		tb.Interval = time.Minute
		fmt.Println("Invalid TokenBucket interval value. Set to default:", tb.Interval)
	}
}
