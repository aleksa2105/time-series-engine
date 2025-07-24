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

	c := chunk.NewTimestampChunk(PageSize, "tests/testTimestamp")
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

	p, err := page.DeserializeTimestampPage(serializedBytes)
	tsp, _ := p.(*page.TimestampPage)
	if err != nil {
		t.Error(err)
	}
	if tsp.Metadata.Count != uint64(numEntries) {
		t.Errorf("Expected deserialized count %d, got %d", numEntries, tsp.Metadata.Count)
	}
	if len(tsp.Entries) != int(tsp.Metadata.Count) {
		t.Errorf("Expected deserialized count of entries %d, got %d", numEntries, len(tsp.Entries))
	}

	fmt.Println("MIN Value")
	fmt.Println(time.Unix(int64(tsp.Metadata.MinValue), 0).Format("2006-01-02 15:04:05"))

	fmt.Println("MAX Value")
	fmt.Println(time.Unix(int64(tsp.Metadata.MaxValue), 0).Format("2006-01-02 15:04:05"))
	fmt.Println()

	for _, e := range tsp.Entries {
		tse, ok := e.(*entry.TimestampEntry)
		if !ok {
			t.Errorf("Expected timestamp entry %v to be a TimestampEntry", e)
		}
		fmt.Println(time.Unix(int64(tse.Value), 0).Format("2006-01-02 15:04:05"))
	}
}
