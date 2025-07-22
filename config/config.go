package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type MemTableConfig struct {
	NumOfInstances uint64 `yaml:"num_of_instances"`
	MaxSize        uint64 `yaml:"max_size"`
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
	ParquetConfig  `yaml:"parquet"`
	WALConfig      `yaml:"wal"`
}

func LoadConfiguration() Config {
	fmt.Println("Loading configuration...")

	// load system configuration
	sysConfigFile, err := os.Open("C:\\Users\\Stefan\\Desktop\\time-series-engine\\time-series-engine\\config\\sys_config.yaml")
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
}
