package tests

import (
	"fmt"
	"testing"
	"time"
	"time-series-engine/config"
	"time-series-engine/internal/disk/chunk"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

func TestTimestampPage(t *testing.T) {
	const PageSize uint64 = 1024 // 1KB

	c := chunk.NewTimestampChunk(PageSize)
	pm := page.NewManager(config.PageConfig{PageSize: PageSize})

	numEntries := 20
	for i := 0; i < numEntries; i++ {
		timeNow := uint64(time.Now().Unix())
		c.Add(pm, timeNow)
		time.Sleep(time.Second)
	}

	if c.ActivePage.Metadata.Count != uint64(numEntries) {
		t.Errorf("Expected count %d, got %d", numEntries, c.ActivePage.Metadata.Count)
	}

	serializedBytes := c.ActivePage.Serialize()

	md, entries, err := page.DeserializeTimestampPage(serializedBytes)
	if err != nil {
		t.Error(err)
	}
	if md.Count != uint64(numEntries) {
		t.Errorf("Expected deserialized count %d, got %d", numEntries, md.Count)
	}
	if len(entries) != numEntries {
		t.Errorf("Expected deserialized count of entries %d, got %d", numEntries, len(entries))
	}

	fmt.Println("MIN Value")
	fmt.Println(time.Unix(int64(md.MinValue), 0).Format("2006-01-02 15:04:05"))

	fmt.Println("MAX Value")
	fmt.Println(time.Unix(int64(md.MaxValue), 0).Format("2006-01-02 15:04:05"))
	fmt.Println()

	for _, e := range entries {
		tse, ok := e.(*entry.TimestampEntry)
		if !ok {
			t.Errorf("Expected timestamp entry %v to be a TimestampEntry", e)
		}
		fmt.Println(time.Unix(int64(tse.Value), 0).Format("2006-01-02 15:04:05"))
	}
}
