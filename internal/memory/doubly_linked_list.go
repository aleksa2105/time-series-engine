package memory

import (
	"fmt"
	"time-series-engine/internal"
)

type node struct {
	Point *internal.Point
	Prev  *node
	Next  *node
}

func newNode(point *internal.Point, prev, next *node) *node {
	return &node{
		Point: point,
		Prev:  prev,
		Next:  next,
	}
}

type DoublyLinkedList struct {
	Header  *node
	Trailer *node
	Size    uint64
}

func NewDoublyLinkedList() *DoublyLinkedList {
	header := newNode(nil, nil, nil)
	trailer := newNode(nil, nil, nil)

	header.Next = trailer
	trailer.Prev = header

	return &DoublyLinkedList{
		Header:  header,
		Trailer: trailer,
		Size:    0,
	}
}

func (dll *DoublyLinkedList) IsEmpty() bool {
	return dll.Size == 0
}

func (dll *DoublyLinkedList) FirstPoint() (*internal.Point, error) {
	if dll.IsEmpty() {
		return nil, fmt.Errorf("list is empty")
	}
	return dll.Header.Next.Point, nil
}
func (dll *DoublyLinkedList) LastPoint() (*internal.Point, error) {
	if dll.IsEmpty() {
		return nil, fmt.Errorf("list is empty")
	}
	return dll.Trailer.Prev.Point, nil
}

func (dll *DoublyLinkedList) Insert(point *internal.Point) {
	lastNode := dll.Trailer.Prev

	nodeToAdd := newNode(point, lastNode, dll.Trailer)
	lastNode.Next = nodeToAdd
	dll.Trailer.Prev = nodeToAdd

	dll.Size += 1
}
func (dll *DoublyLinkedList) DeleteRange(minTimestamp, maxTimestamp uint64) uint64 {
	// Finding node that is at beginning of range [minTimestamp, maxTimestamp]:
	lowerNode := dll.Header.Next
	for lowerNode != dll.Trailer {
		curTimestamp := lowerNode.Point.Timestamp
		if minTimestamp <= curTimestamp && curTimestamp <= maxTimestamp {
			break
		}
		lowerNode = lowerNode.Next
	}
	// Check if we found any:
	if lowerNode == dll.Trailer {
		return 0
	}

	// Finding node that is at the end of the range:
	higherNode := lowerNode
	var deleteCount uint64 = 0
	for higherNode != dll.Trailer {
		curTimestamp := higherNode.Point.Timestamp
		if curTimestamp > maxTimestamp {
			higherNode = higherNode.Prev
			break
		}
		higherNode = higherNode.Next
		deleteCount += 1
	}

	dll.Size -= deleteCount

	before := lowerNode.Prev
	if higherNode != dll.Trailer {
		// Delete just a part the list:
		after := higherNode.Next
		lowerNode.Prev = nil
		higherNode.Next = nil
		before.Next = after
		after.Prev = before
	} else {
		// Delete all remaining nodes since they are all in range:
		before.Next = dll.Trailer
		dll.Trailer.Prev = before
	}

	return deleteCount
}
func (dll *DoublyLinkedList) GetPointsInInterval(minTimestamp, maxTimestamp uint64) []*internal.Point {
	points := make([]*internal.Point, 0, dll.Size)

	curNode := dll.Header.Next
	for curNode != dll.Trailer {
		curTimestamp := curNode.Point.Timestamp
		if minTimestamp <= curTimestamp && curTimestamp <= maxTimestamp {
			points = append(points, curNode.Point)
		}
		curNode = curNode.Next
	}

	return points
}

func (dll *DoublyLinkedList) GetSortedPoints() []*internal.Point {
	points := make([]*internal.Point, 0, dll.Size)

	// Traversing the list:
	curNode := dll.Header.Next
	for curNode != dll.Trailer {
		points = append(points, curNode.Point)
		curNode = curNode.Next
	}

	return points
}
