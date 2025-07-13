package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type MemtableConfig struct {
	Storage        string `yaml:"storage"`
	NumOfInstances uint64 `yaml:"num_of_instances"`
	MaxItems       uint64 `yaml:"max_items"`
	BTreeOrder     uint64 `yaml:"b_tree_order"`
}

type BlockManagerConfig struct {
	BlockSize      uint64 `yaml:"block_size"`
	FilenameLength uint64 `yaml:"filename_length"`
	SplitFactor    uint64 `yaml:"split_factor"`
}

type SSTableConfig struct {
	Format          string `yaml:"format"`
	SummaryFactor   uint64 `yaml:"summary_factor"`
	Compression     bool   `yaml:"compression"`
	MerkleHashBytes uint64 `yaml:"merkle_hash_bytes"`
	DirPath         string `yaml:"dir_path"`
}

type WALConfig struct {
	SegmentSizeInBlocks uint64 `yaml:"segment_size_in_blocks"`
	LogsDirPath         string `yaml:"logs_dir_path"`
	UnstagedOffset      uint64 `yaml:"unstaged_offset"`
}

type CacheConfig struct {
	MaxItems uint64 `yaml:"max_items"`
}

type BufferPoolConfig struct {
	MaxBlocks uint64 `yaml:"max_blocks"`
}

type LSMConfig struct {
	MaxLevels           uint64 `yaml:"max_levels"`
	CompactionAlgorithm string `yaml:"compaction_algorithm"`
	Leveled             struct {
		Level0Size      uint64 `yaml:"level0_size"`
		LevelMultiplier uint64 `yaml:"level_multiplier"`
	} `yaml:"leveled"`
	SizeTiered struct {
		SizeThreshold uint64 `yaml:"size_threshold"`
	} `yaml:"size-tiered"`
}

type TBConfig struct {
	User     string        `yaml:"user"`
	Capacity uint64        `yaml:"capacity"`
	Interval time.Duration `yaml:"interval"`
}

type ReservedPrefixes struct {
	BloomFilter    string `yaml:"bloom_filter"`
	CountMinSketch string `yaml:"count_min_sketch"`
	HyperLogLog    string `yaml:"hyper_log_log"`
	SimHash        string `yaml:"simhash"`
	TokenBucket    string `yaml:"token_bucket"`
}

type Config struct {
	MemtableConfig     `yaml:"memtable"`
	BlockManagerConfig `yaml:"block_manager"`
	SSTableConfig      `yaml:"sstable"`
	WALConfig          `yaml:"wal"`
	CacheConfig        `yaml:"cache"`
	BufferPoolConfig   `yaml:"buffer_pool"`
	LSMConfig          `yaml:"lsm"`
	TBConfig           `yaml:"token_bucket"`
	ReservedPrefixes   `yaml:"reserved_prefixes"`
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

// setDefaults will fill empty and incorrect values with default ones
func (c *Config) setDefaults() {
	// Memtable
	mc := &c.MemtableConfig
	if mc.Storage != "hash-map" && mc.Storage != "b-tree" && mc.Storage != "skip-list" {
		mc.Storage = "hash-map"
		fmt.Printf("Invalid Memtable storage value. Set to default: %s\n", mc.Storage)
	}
	if mc.NumOfInstances < 1 || mc.NumOfInstances > 4 {
		mc.NumOfInstances = 3
		fmt.Printf("Invalid Memtable num_of_instances value. Set to default: %d\n", mc.NumOfInstances)
	}
	if mc.MaxItems < 2 || mc.MaxItems > 10000 {
		mc.MaxItems = 1000
		fmt.Printf("Invalid Memtable max_items value. Set to default: %d\n", mc.MaxItems)
	}
	if mc.BTreeOrder < 8 || mc.BTreeOrder > 32 {
		mc.BTreeOrder = 8
		fmt.Printf("Invalid Memtable b_tree_order value. Set to default: %d\n", mc.BTreeOrder)
	}

	// BlockManager
	bmc := &c.BlockManagerConfig
	if bmc.BlockSize < 100 || bmc.BlockSize > 16000 {
		bmc.BlockSize = 8000
		fmt.Printf("Invalid BlockManager block_size value. Set to default: %d\n", bmc.BlockSize)
	}
	if bmc.FilenameLength < 1 || bmc.FilenameLength > 8 {
		bmc.FilenameLength = 4
	}
	if bmc.SplitFactor < 1 || bmc.SplitFactor > 100 {
		bmc.SplitFactor = 34
	}

	// SSTable
	sc := &c.SSTableConfig
	if sc.Format != "separate-files" && sc.Format != "same-file" {
		sc.Format = "separate-files"
		fmt.Printf("Invalid SSTable format value. Set to default: %s\n", sc.Format)
	}
	if sc.SummaryFactor < 4 || sc.SummaryFactor > 16 {
		sc.SummaryFactor = 8
		fmt.Printf("Invalid SSTable summary_factor value. Set to default: %d\n", sc.SummaryFactor)
	}
	if sc.Compression != false && sc.Compression != true {
		sc.Compression = false
		fmt.Println("Invalid SSTable compression value. Set to default:", sc.Compression)
	}
	if sc.MerkleHashBytes < 50 || sc.MerkleHashBytes > 1000 {
		sc.MerkleHashBytes = 100
	}
	if sc.DirPath != "./internal/disk/sstable/data" {
		sc.DirPath = "./internal/disk/sstable/data"
	}

	// WAL
	wc := &c.WALConfig
	if wc.SegmentSizeInBlocks < 2 || wc.SegmentSizeInBlocks > 4096 {
		wc.SegmentSizeInBlocks = 256
		fmt.Printf("Invalid WAL segment_size_in_blocks value. Set to default: %d\n", wc.SegmentSizeInBlocks)
	}
	if wc.LogsDirPath != "./internal/disk/write-ahead-log/logs" {
		wc.LogsDirPath = "./internal/disk/write-ahead-log/logs"
	}

	// Cache
	cc := &c.CacheConfig
	if cc.MaxItems < 1000 || cc.MaxItems > 10000 {
		cc.MaxItems = 1000
		fmt.Printf("Invalid Cache max_items value. Set to default: %d\n", cc.MaxItems)
	}

	// BufferPool
	bpc := &c.BufferPoolConfig
	if bpc.MaxBlocks < 100 || bpc.MaxBlocks > 1000 {
		bpc.MaxBlocks = 100
		fmt.Printf("Invalid BufferPool max_blocks value. Set to default: %d\n", bpc.MaxBlocks)

	}

	// LSM
	lc := &c.LSMConfig
	if lc.MaxLevels < 1 || lc.MaxLevels > 6 {
		lc.MaxLevels = 6
		fmt.Printf("Invalid LSM max_levels value. Set to default: %d\n", lc.MaxLevels)
	}
	if lc.CompactionAlgorithm != "leveled" && lc.CompactionAlgorithm != "size-tiered" {
		lc.CompactionAlgorithm = "leveled"
		fmt.Println("Invalid LSM compaction_algorithm value. Set to default:", lc.CompactionAlgorithm)
	}

	// Leveled
	if lc.Leveled.Level0Size < 1 || lc.Leveled.Level0Size > 10 {
		lc.Leveled.Level0Size = 10
		fmt.Println("Invalid LSM level0_size value. Set to default:", lc.Leveled.Level0Size)
	}
	if lc.Leveled.LevelMultiplier < 1 || lc.Leveled.LevelMultiplier > 10 {
		lc.Leveled.LevelMultiplier = 10
		fmt.Println("Invalid LSM level_multiplier value. Set to default:", lc.Leveled.LevelMultiplier)
	}

	// Size-Tiered
	if lc.SizeTiered.SizeThreshold < 1 || lc.SizeTiered.SizeThreshold > 4 {
		lc.SizeTiered.SizeThreshold = 4
		fmt.Println("Invalid LSM size_threshold value. Set to default:", lc.SizeTiered.SizeThreshold)
	}

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

func (c *Config) CheckPrefix(key string) bool {
	bf := c.ReservedPrefixes.BloomFilter
	n := len(bf)
	if len(key) >= n {
		if key[:n] == bf {
			return true
		}
	}

	cms := c.ReservedPrefixes.CountMinSketch
	n = len(cms)
	if len(key) >= n {
		if key[:n] == cms {
			return true
		}
	}

	hll := c.ReservedPrefixes.HyperLogLog
	n = len(hll)
	if len(key) >= n {
		if key[:n] == hll {
			return true
		}
	}

	sh := c.ReservedPrefixes.SimHash
	n = len(sh)
	if len(key) >= n {
		if key[:n] == sh {
			return true
		}
	}

	tb := c.ReservedPrefixes.TokenBucket
	n = len(tb)
	if len(key) >= n {
		if key[:n] == tb {
			return true
		}
	}

	return false
}
