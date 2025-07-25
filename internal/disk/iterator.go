package disk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/page/page_manager"
)

type Iterator struct {
	ActivePage        page.Page
	CurrentEntryIndex uint64
	CurrentPageOffset uint64
	Filename          string
	PageManager       *page_manager.Manager
}

func NewIterator(pm *page_manager.Manager, filename string) (*Iterator, error) {
	it := &Iterator{
		ActivePage:        nil,
		CurrentEntryIndex: 0,
		CurrentPageOffset: 0,
		Filename:          filename,
		PageManager:       pm,
	}

	err := it.LoadNextPage()
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (it *Iterator) LoadNextPage() error {
	bytes, err := it.PageManager.ReadPage(it.Filename, int64(it.CurrentPageOffset))
	if err != nil {
		return err
	}

	p, err := page.DeserializeValuePage(bytes)
	if err != nil {
		return err
	}

	it.ActivePage = p
	it.CurrentPageOffset += it.PageManager.Config.PageSize
	return nil
}

func (it *Iterator) Next() (entry.Entry, error) {
	if it.CurrentEntryIndex >= it.ActivePage.EntryCount() {
		err := it.LoadNextPage()
		if err != nil {
			return nil, err
		}
		it.CurrentEntryIndex = 0
	}

	e := it.ActivePage.GetEntries()[it.CurrentEntryIndex]
	it.CurrentEntryIndex++

	return e, nil
}

func (it *Iterator) Skip(minTimestamp uint64, maxTimestamp uint64) (uint64, error) {
	count := uint64(0)
	for {
		meta := it.ActivePage.GetMetadata()
		if DoIntervalsOverlap(minTimestamp, maxTimestamp, meta.MinValue, meta.MaxValue) {
			break
		}
		count += meta.Count

		err := it.LoadNextPage()
		if err != nil {
			return 0, err
		}
	}

	for {
		e, err := it.Next()
		if err != nil {
			return 0, err
		}

		if e.GetValue() >= minTimestamp {
			break
		}
		count++
	}

	return count, nil
}

func (it *Iterator) Advance(count uint64) error {
	for count > 0 {
		meta := it.ActivePage.GetMetadata()
		if meta.Count > count {
			break
		}
		count -= meta.Count
		err := it.LoadNextPage()
		if err != nil {
			return err
		}
	}

	for i := uint64(0); i < count; i++ {
		_, err := it.Next()
		if err != nil {
			return err
		}
	}

	return nil
}
