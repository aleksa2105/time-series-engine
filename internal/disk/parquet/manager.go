package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/page"
)

type Manager struct {
	ActiveParquetHash string
	ActiveParquet     *Parquet
	Config            *config.ParquetConfig
	PageManager       *page.Manager
	TimeWindowPath    string
	ParquetIndex      uint64
}

func NewManager(cfg *config.ParquetConfig, pm *page.Manager, path string) *Manager {
	return &Manager{
		ActiveParquetHash: "",
		ActiveParquet:     nil,
		Config:            cfg,
		PageManager:       pm,
		TimeWindowPath:    path,
		ParquetIndex:      0,
	}
}

func (m *Manager) createParquetDirectoryPath() (string, error) {
	pName := fmt.Sprintf("parquet%04d", m.ParquetIndex)
	pPath := filepath.Join(m.TimeWindowPath, pName)
	err := os.Mkdir(pPath, 0755)
	if err != nil {
		return "", err
	}

	return pPath, nil
}

func (m *Manager) Close() error {
	err := m.ActiveParquet.Close()
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) FlushSeries(tsHash string, points []*internal.Point) error {
	if m.ActiveParquetHash != tsHash {
		if m.ActiveParquet != nil {
			err := m.ActiveParquet.Close()
			if err != nil {
				return err
			}
		}

		foundParquet, err := m.findParquetDirectory(tsHash)
		if err != nil {
			return err
		}

		if foundParquet != nil {
			m.ActiveParquet = foundParquet
		} else {
			var path string
			path, err = m.createParquetDirectoryPath()
			m.ActiveParquet, err = NewParquet(tsHash, m.Config, m.PageManager, path)
			if err != nil {
				return err
			}
			m.ParquetIndex++
		}

		m.ActiveParquetHash = tsHash
	}

	for _, p := range points {
		if err := m.ActiveParquet.AddPoint(p); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) FlushAll(series map[string][]*internal.Point) error {
	for ts, points := range series {
		if err := m.FlushSeries(ts, points); err != nil {
			return err
		}
	}

	if m.ActiveParquet != nil {
		err := m.ActiveParquet.Close()
		if err != nil {
			return err
		}

		m.ActiveParquet = nil
		m.ActiveParquetHash = ""
	}

	return nil
}

// findParquetDirectory : search if already exists parquet file
// with appropriate time series hash in actual time window
//   - returns parquet if it already exists (nil otherwise), and error indicator
func (m *Manager) findParquetDirectory(timeSeriesHash string) (*Parquet, error) {
	entries, err := os.ReadDir(m.TimeWindowPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read time window directory: %w", m.TimeWindowPath)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		parquetDir := filepath.Join(m.TimeWindowPath, entry.Name())
		metaPath := filepath.Join(parquetDir, "metadata.db")

		data, err := m.PageManager.ReadStructure(metaPath, 0)
		if err != nil {
			return nil, err
		}

		meta, err := DeserializeParquetMetadata(data)
		if err != nil {
			return nil, err
		}

		if meta.TimeSeriesHash == timeSeriesHash {
			p, err := LoadParquet(meta, m.Config, m.PageManager, parquetDir)
			if err != nil {
				return nil, err
			}

			return p, nil
		}
	}

	return nil, nil
}

func (m *Manager) Update(twPath string) {
	m.ActiveParquetHash = ""
	m.ActiveParquet = nil
	m.TimeWindowPath = twPath
	m.ParquetIndex = 0
}
