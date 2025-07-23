package chunk

import (
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
)

type StringChunk struct {
	Dictionary    *Dictionary
	ActivePage    *page.StringPage
	IdGenerator   uint64
	FilePath      string
	CurrentOffset uint64
}

func NewStringChunk(pageSize uint64, filePath string) *StringChunk {
	return &StringChunk{
		Dictionary:    NewDictionary(),
		ActivePage:    page.NewStringPage(pageSize),
		IdGenerator:   1,
		FilePath:      filePath,
		CurrentOffset: 0,
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
		pm.WritePage(sc.ActivePage, sc.FilePath, int64(sc.CurrentOffset))
		sc.CurrentOffset += pm.Config.PageSize

		sc.ActivePage = page.NewStringPage(pm.Config.PageSize)
	}

	sc.ActivePage.Add(newEntry)
}

func (sc *StringChunk) Save(pm *page.Manager) {
	pm.WritePage(sc.ActivePage, sc.FilePath, int64(sc.CurrentOffset))
	sc.CurrentOffset += pm.Config.PageSize

	pm.WriteStructure(sc.Dictionary.Serialize(), sc.FilePath, int64(sc.CurrentOffset))
}
