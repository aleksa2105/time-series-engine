package page

import (
	"fmt"
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

func (m *Manager) Write(p Page) {
	fmt.Println(p.Serialize())
}
func (m *Manager) Read() {}
