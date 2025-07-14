package page

import "time-series-engine/config"

type Manager struct {
	Config config.PageConfig
}

func NewManager(config config.PageConfig) *Manager {
	return &Manager{
		Config: config,
	}
}
