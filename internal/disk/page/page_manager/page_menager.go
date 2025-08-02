package page_manager

import (
	"encoding/binary"
	"os"
	"time-series-engine/config"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/memory/buffer_pool"
)

type Manager struct {
	Config     config.PageConfig
	bufferPool *buffer_pool.BufferPool
}

func NewManager(config config.PageConfig) *Manager {
	return &Manager{
		Config:     config,
		bufferPool: buffer_pool.NewBufferPool(config.BufferPoolCapacity),
	}
}

func (m *Manager) WritePage(p page.Page, path string, offset int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	//fmt.Printf("\nFlush: %s, %d\n", path, offset)
	//for _, e := range p.GetEntries() {
	//	fmt.Println(e)
	//}

	bytes := p.Serialize()
	_, err = file.Write(bytes)
	if err != nil {
		return err
	}
	found := m.bufferPool.Get(path, offset)
	if found != nil {
		m.bufferPool.Put(bytes, path, offset)
	}

	return nil
}

func (m *Manager) ReadPage(path string, offset int64) ([]byte, error) {
	p := m.bufferPool.Get(path, offset)
	if p != nil {
		return p, nil
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, m.Config.PageSize)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}

	m.bufferPool.Put(bytes, path, offset)

	return bytes, nil
}

func (m *Manager) WriteStructure(data []byte, path string, offset int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	lengthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(data)))
	_, err = file.Write(lengthBytes)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) ReadStructure(path string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, 8)
	_, err = file.Read(lengthBytes)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint64(lengthBytes)
	bytes := make([]byte, length)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (m *Manager) ReadBytes(path string, offset int64, length int64) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, length)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (m *Manager) WriteBytes(path string, offset int64, bytes []byte) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = file.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) CreateFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	return nil
}

func (m *Manager) RemoveFile(filename string) error {
	err := m.bufferPool.Remove(filename)
	if err != nil {
		return err
	}

	err = os.RemoveAll(filename)
	if err != nil {
		return err
	}
	return nil
}
