package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

type StringChunk struct {
	Dictionary  *Dictionary
	ActivePage  *page.StringPage
	IdGenerator uint64
}

func NewStringChunk(pageSize uint64) *StringChunk {
	return &StringChunk{
		Dictionary:  NewDictionary(),
		ActivePage:  page.NewStringPage(pageSize),
		IdGenerator: 1,
	}
}

func (sc *StringChunk) Add(pm *page.Manager, value string) {
	encodedValue, found := sc.Dictionary.KeyToId[value]
	if !found {
		sc.Dictionary.KeyToId[value] = sc.IdGenerator
		sc.Dictionary.IdToKey[sc.IdGenerator] = value

		encodedValue = sc.IdGenerator
		sc.IdGenerator++
	}

	if len(sc.ActivePage.Entries) > 0 {
		previousEntry := sc.ActivePage.Entries[len(sc.ActivePage.Entries)-1]
		if previousEntry.Value == encodedValue {
			previousEntry.NumRepetitions++
			sc.ActivePage.Metadata.Count++
			return
		}
	}

	newEntry := entry.NewStringEntry(encodedValue)
	if newEntry.Size() > sc.ActivePage.Padding {
		pm.Write(sc.ActivePage)
		sc.ActivePage = page.NewStringPage(pm.Config.PageSize)
	}

	sc.ActivePage.AddEntry(newEntry)
}

func (sc *StringChunk) AddNullEntry(pm *page.Manager) {
	if len(sc.ActivePage.Entries) > 0 {
		previousEntry := sc.ActivePage.Entries[len(sc.ActivePage.Entries)-1]
		if previousEntry.Value == 0 {
			previousEntry.NumRepetitions++
			sc.ActivePage.Metadata.Count++
			return
		}
	}

	newEntry := entry.NewStringEntry(0)
	if newEntry.Size() > sc.ActivePage.Padding {
		pm.Write(sc.ActivePage)
		sc.ActivePage = page.NewStringPage(pm.Config.PageSize)
	}

	sc.ActivePage.AddEntry(newEntry)
}

func (sc *StringChunk) AddNullEntries(numRepetitions uint64) {
	e := entry.NewStringEntry(0)
	e.NumRepetitions = numRepetitions
	sc.ActivePage.AddEntry(e)
}
