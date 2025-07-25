package write_ahead_log

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time-series-engine/config"
	"time-series-engine/internal"
	"time-series-engine/internal/disk/entry"
	"time-series-engine/internal/disk/page"
	"time-series-engine/internal/disk/page/page_manager"
)

const INDEX = 8

type WriteAheadLog struct {
	segments        []string
	activeSegment   string
	activePageIndex uint64
	activePage      *page.WALPage
	pageManager     *page_manager.Manager
	config          *config.WALConfig
}

func NewWriteAheadLog(c *config.WALConfig, pm *page_manager.Manager) *WriteAheadLog {
	return &WriteAheadLog{
		segments:        make([]string, 0),
		activeSegment:   "",
		activePageIndex: 0,
		activePage:      nil,
		pageManager:     pm,
		config:          c,
	}
}

func (wal *WriteAheadLog) Put(ts *internal.TimeSeries, p *internal.Point) error {
	walEnt := entry.NewWALPutEntry(ts, p)
	if walEnt.Size() > wal.activePage.PaddingSize() {
		err := wal.changePage()
		if err != nil {
			return err
		}
	}
	wal.activePage.Add(walEnt)
	err := wal.writeWalBlock()
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) Delete(ts *internal.TimeSeries, minTimestamp, maxTimestamp uint64) error {
	walEnt := entry.NewWALDeleteEntry(ts, minTimestamp, maxTimestamp)
	if walEnt.Size() > wal.activePage.PaddingSize() {
		err := wal.changePage()
		if err != nil {
			return err
		}
	}
	wal.activePage.Add(walEnt)
	err := wal.writeWalBlock()
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) writeWalBlock() error {
	filename := wal.config.LogsDirPath + "/" + wal.activeSegment
	offset := INDEX + wal.activePageIndex*wal.pageManager.Config.PageSize

	err := wal.pageManager.WritePage(wal.activePage, filename, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

func (wal *WriteAheadLog) LoadWal() error {
	files, err := os.ReadDir(wal.config.LogsDirPath)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		err := wal.createNewSegment()
		if err != nil {
			return err
		}
		wal.SetUnstagedOffset(INDEX)
		return nil
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		segmentIndexBytes, err := wal.pageManager.ReadBytes(wal.config.LogsDirPath+"/"+file.Name(), 0, INDEX)
		if err != nil {
			return err
		}

		segmentIndex := binary.BigEndian.Uint64(segmentIndexBytes)

		segment := fmt.Sprint(segmentIndex)
		segment = fmt.Sprintf("wal_%s%s.log",
			strings.Repeat("0", int(wal.pageManager.Config.FilenameLength)-len(segment)), segment)
		wal.segments = append(wal.segments, segment)

		err = os.Rename(wal.config.LogsDirPath+"/"+file.Name(), wal.config.LogsDirPath+"/"+segment)
		if err != nil {
			return err
		}
	}

	insertionSort(&wal.segments)
	wal.activeSegment = wal.segments[len(wal.segments)-1]
	activeSegmentFilename := wal.config.LogsDirPath + "/" + wal.activeSegment

	stat, err := os.Stat(activeSegmentFilename)
	if err != nil {
		return err
	}

	fileSize := stat.Size()
	if fileSize <= int64(INDEX) {
		wal.activePage = page.NewWALPage(wal.pageManager.Config.PageSize)
		wal.activePageIndex = 0
		return nil
	}

	offset := fileSize - int64(wal.pageManager.Config.PageSize)
	wal.activePage = page.NewWALPage(wal.pageManager.Config.PageSize)
	activePageBytes, err := wal.pageManager.ReadBytes(activeSegmentFilename, offset, int64(wal.pageManager.Config.PageSize))

	wal.activePage, err = page.DeserializeWALPage(activePageBytes)
	if err != nil {
		return err
	}

	lastPageIndex := uint64(fileSize-INDEX) / wal.pageManager.Config.PageSize
	if lastPageIndex != 0 {
		lastPageIndex -= 1
	}
	wal.activePageIndex = lastPageIndex

	return nil
}

func (wal *WriteAheadLog) createNewSegment() error {
	segment := fmt.Sprint(wal.LastSegmentIndex() + 1)
	segment = fmt.Sprintf("wal_%0*s.log", wal.pageManager.Config.FilenameLength, segment)

	filename := wal.config.LogsDirPath + "/" + segment
	err := wal.pageManager.CreateFile(filename)
	if err != nil {
		return err
	}

	bytes := make([]byte, INDEX)
	binary.BigEndian.PutUint64(bytes, wal.LastSegmentIndex()+1)
	err = wal.pageManager.WriteBytes(filename, 0, bytes)
	if err != nil {
		return err
	}

	wal.segments = append(wal.segments, segment)
	wal.activeSegment = segment
	wal.activePage = page.NewWALPage(wal.pageManager.Config.PageSize)
	wal.activePageIndex = 0
	return nil
}

func (wal *WriteAheadLog) changePage() error {
	newSegBool := false
	if wal.IsFullSegment() {
		err := wal.createNewSegment()
		if err != nil {
			return err
		}
		newSegBool = true
	}

	if !newSegBool {
		wal.activePage = page.NewWALPage(wal.pageManager.Config.PageSize)
		wal.activePageIndex += 1
	}

	return nil
}

func (wal *WriteAheadLog) IsFullSegment() bool {
	return wal.activePageIndex == wal.config.SegmentSizeInPages-1
}

func (wal *WriteAheadLog) SegmentsNumber() uint64 {
	return uint64(len(wal.segments))
}

func (wal *WriteAheadLog) ActiveSegment() string {
	return wal.activeSegment
}

func (wal *WriteAheadLog) ActiveSegmentOffset() uint64 {
	return INDEX + (wal.activePageIndex+1)*wal.pageManager.Config.PageSize - wal.activePage.PaddingSize()
}

func (wal *WriteAheadLog) UnstagedOffset() uint64 {
	return wal.config.UnstagedOffset
}

func (wal *WriteAheadLog) SetUnstagedOffset(offset uint64) {
	wal.config.UnstagedOffset = offset
}

func (wal *WriteAheadLog) LastSegmentIndex() uint64 {
	parts := strings.Split(wal.activeSegment, "_")
	if len(parts) != 2 {
		return 0
	}
	indexPart := strings.TrimSuffix(parts[1], ".log")

	index, _ := strconv.ParseUint(indexPart, 10, 64)
	return index
}

func (wal *WriteAheadLog) SegmentName(index uint64) string {
	return wal.segments[index]
}

func (wal *WriteAheadLog) SegmentFilename(index uint64) string {
	return wal.config.LogsDirPath + "/" + wal.segments[index]
}

func insertionSort(array *[]string) {
	for i := 0; i < len(*array); i++ {
		key := (*array)[i]
		j := i - 1

		for j >= 0 && (*array)[j] > key {
			(*array)[j+1] = (*array)[j]
			j--
		}
		(*array)[j+1] = key
	}
}
