package chunk

import (
	"time-series-engine/internal/disk/page"
)

type TimestampChunk struct {
	ActivePage *page.Page
}

func NewTimestampChunk(pageSize uint64) *TimestampChunk {
	return &TimestampChunk{
		ActivePage: page.NewPage(pageSize),
	}
}

func (c *TimestampChunk) Add(value uint64) {
	c.ActivePage.AddEntry(value)
}
