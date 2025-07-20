package page

import (
	"fmt"
	"os"
	"time-series-engine/config"
)

type Manager struct {
	Config config.PageConfig
}

func NewManager(config config.PageConfig) *Manager {
	return &Manager{
		Config: config,
	}
}

func (m *Manager) WritePage(p Page, path string, offset int64) {
	fmt.Println(fmt.Sprintf("Write page: %s, %d", path, offset))
	switch p.(type) {
	case *TimestampPage:
		tp := p.(*TimestampPage)
		for _, curr := range tp.Entries {
			fmt.Println(curr.Value)
		}
	case *ValuePage:
		tp := p.(*ValuePage)
		for _, curr := range tp.Entries {
			fmt.Println(curr.Value)
		}
	default:
		fmt.Println("Nepoznat tip")
	}
}
func (m *Manager) Read() {}

func (m *Manager) WriteStructure(data []byte, path string, offset int64) {

}

func (m *Manager) CreateFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}
