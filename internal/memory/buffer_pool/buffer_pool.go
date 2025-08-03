package buffer_pool

import (
	"fmt"
	"strings"
)

type PageKey struct {
	Filename string
	Offset   int64
}

type BufferPool struct {
	capacity   uint64
	hashMap    map[PageKey]*DLLNode
	doublyList DLL
}

func NewBufferPool(c uint64) *BufferPool {
	return &BufferPool{
		capacity:   c,
		hashMap:    make(map[PageKey]*DLLNode),
		doublyList: DLL{},
	}
}

func (bp *BufferPool) Get(path string, offset int64) []byte {
	key := PageKey{Filename: path, Offset: offset}
	n, ok := bp.hashMap[key]
	if !ok {
		return nil
	}

	bp.doublyList.MoveToFront(n)
	return n.page
}

func (bp *BufferPool) Put(p []byte, filename string, offset int64) {
	pk := PageKey{Filename: filename, Offset: offset}
	if n, ok := bp.hashMap[pk]; ok {
		n.page = p
		n.pageKey = pk
		bp.doublyList.MoveToFront(n)
		return
	}

	if bp.IsFull() {
		oldest := bp.doublyList.head
		delete(bp.hashMap, oldest.pageKey)
		bp.doublyList.Delete()
	}

	bp.doublyList.Put(p)
	bp.hashMap[pk] = bp.doublyList.tail
}

func (bp *BufferPool) IsFull() bool {
	return bp.doublyList.PageCount == bp.capacity
}

func (bp *BufferPool) Remove(filename string) error {
	for key, node := range bp.hashMap {
		if strings.HasPrefix(key.Filename, filename) {
			bp.doublyList.DeleteNode(node)
			delete(bp.hashMap, key)
		}
	}
	return nil
}

type DLL struct {
	head      *DLLNode
	tail      *DLLNode
	PageCount uint64
}

func (dll *DLL) Put(p []byte) {
	n := NewDLLNode(p)

	if dll.head == nil {
		dll.head = n
		dll.tail = n
	} else {
		dll.tail.next = n
		n.prev = dll.tail
		dll.tail = n
	}
	dll.PageCount++
}

func (dll *DLL) Delete() {
	if dll.head == nil {
		fmt.Println("Linked list is empty")
	}
	if dll.PageCount == 1 {
		dll.head = nil
		dll.tail = nil
	} else {
		dll.head = dll.head.next
		dll.head.prev = nil
	}
	dll.PageCount--
}

func (dll *DLL) MoveToFront(node *DLLNode) {
	if node == dll.tail || dll.PageCount == 1 || node == nil {
		return
	}

	if node == dll.head {
		dll.head = node.next
		dll.head.prev = nil
	} else {
		if node.prev != nil {
			node.prev.next = node.next
		}
		if node.next != nil {
			node.next.prev = node.prev
		}
	}

	node.prev = dll.tail
	node.next = nil
	dll.tail.next = node
	dll.tail = node
}

func (dll *DLL) DeleteNode(node *DLLNode) {
	if node == nil {
		return
	}

	if node == dll.head {
		dll.head = node.next
		if dll.head != nil {
			dll.head.prev = nil
		}
	} else if node == dll.tail {
		dll.tail = node.prev
		if dll.tail != nil {
			dll.tail.next = nil
		}
	} else {
		if node.prev != nil {
			node.prev.next = node.next
		}
		if node.next != nil {
			node.next.prev = node.prev
		}
	}

	dll.PageCount--
	if dll.PageCount == 0 {
		dll.head = nil
		dll.tail = nil
	}
}

type DLLNode struct {
	next    *DLLNode
	prev    *DLLNode
	page    []byte
	pageKey PageKey
}

func NewDLLNode(p []byte) *DLLNode {
	return &DLLNode{
		next:    nil,
		prev:    nil,
		page:    p,
		pageKey: PageKey{Offset: 0, Filename: ""},
	}
}
