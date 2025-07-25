package tests

import (
	"fmt"
	"math/rand"
	"testing"
	"time-series-engine/config"
	"time-series-engine/internal/disk/chunk"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/page/page_manager"
)

func TestValuePage(t *testing.T) {

	const PageSize uint64 = 200

	c := chunk.NewValueChunk(PageSize, "tests/testValue")
	pm := page_manager.NewManager(config.PageConfig{PageSize: PageSize})

	numEntries := 30
	for i := 0; i < numEntries; i++ {
		val := float64(1-2*rand.Intn(2)) * float64(i) * float64(PageSize) * rand.Float64()

		fmt.Println(i, val)
		err := c.Add(pm, val)
		if err != nil {
			t.Error(err)
		}
	}

	serializedBytes := c.ActivePage.Serialize()

	if len(serializedBytes) != int(PageSize) {
		t.Errorf("Serialized size does not match. Expected %d, got %d", PageSize, len(serializedBytes))
	}

	p, err := page.DeserializeValuePage(serializedBytes)
	vp, _ := p.(*page.ValuePage)
	if err != nil {
		t.Fatal(err)
	}
	if vp.Metadata.Count != uint64(numEntries) {
		t.Fatalf("expected %d entries, got %d", numEntries, vp.Metadata.Count)
	}
	if len(vp.Entries) != int(vp.Metadata.Count) {
		t.Fatalf("expected %d entries, got %d", numEntries, len(vp.Entries))
	}

	for i, e := range vp.Entries {
		ve, ok := e.(*entry.ValueEntry)
		if !ok {
			t.Fatal("expected entry to be *entry.ValueEntry")
		}
		fmt.Println("Val", i, ":", ve.Value)
	}

}
