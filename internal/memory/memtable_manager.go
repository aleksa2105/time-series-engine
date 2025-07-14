package memory

import (
	"fmt"
	"time-series-engine/config"
	"time-series-engine/internal"
)

type MemTableManager struct {
	currentTableIndex uint64
	Queue             []*MemTable
	Config            *config.MemTableConfig
}

func NewMemTableManager(c *config.MemTableConfig) *MemTableManager {
	var i uint64 = 0
	tableQueue := make([]*MemTable, c.NumOfInstances)
	for ; i < c.NumOfInstances; i++ {

		tableQueue[i] = NewMemTable()
	}

	return &MemTableManager{
		Queue: tableQueue,
	}
}

func (mm *MemTableManager) Rotate() {
	mm.currentTableIndex = (mm.currentTableIndex + 1) % mm.Config.NumOfInstances
}

func (mm *MemTableManager) Put(point *internal.Point) {
	if mm.Queue[mm.currentTableIndex].IsFull(mm.Config) {
		mm.Rotate()
		if mm.Queue[mm.currentTableIndex].IsFull(mm.Config) {
			fmt.Println("FLUSH")
			mm.Queue[mm.currentTableIndex] = NewMemTable()
		}
	}

	mm.Queue[mm.currentTableIndex].Put(point)
}
