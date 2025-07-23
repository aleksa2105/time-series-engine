package tests

import (
	"fmt"
	"math"
	"testing"
	"time-series-engine/config"
	"time-series-engine/internal/disk/chunk"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

func TestValuePage(t *testing.T) {

	const PageSize uint64 = 100

	c := chunk.NewValueChunk(PageSize)
	pm := page.NewManager(config.PageConfig{PageSize: PageSize})

	numEntries := 10
	c.Add(pm, 1.52131)
	c.Add(pm, 2.553252)
	c.Add(pm, 300000.63413412)
	c.Add(pm, 4.521788181)
	c.Add(pm, 5.2528958181)
	c.Add(pm, 8.551221111)
	c.Add(pm, 12.85721751)
	c.Add(pm, 5.528951)
	c.Add(pm, 21.952718)
	c.Add(pm, 2002.525)

	serializedBytes := c.ActivePage.Serialize()

	if len(serializedBytes) != int(PageSize) {
		t.Errorf("Serialized size does not match. Expected %d, got %d", PageSize, len(serializedBytes))
	}

	md, entries, err := page.DeserializeValuePage(serializedBytes)
	if err != nil {
		t.Fatal(err)
	}
	if md.Count != uint64(numEntries) {
		t.Fatalf("expected %d entries, got %d", numEntries, md.Count)
	}
	if len(entries) != int(md.Count) {
		t.Fatalf("expected %d entries, got %d", numEntries, md.Count)
	}

	fmt.Println()
	fmt.Println("MIN value")
	fmt.Println(math.Float64frombits(md.MinValue))
	fmt.Println("MAX value")
	fmt.Println(math.Float64frombits(md.MaxValue))
	fmt.Println()

	for i, e := range entries {
		ve, ok := e.(*entry.ValueEntry)
		if !ok {
			t.Fatal("expected entry to be *entry.ValueEntry")
		}
		fmt.Println("Val", i, ":", ve.Value)
	}

}
