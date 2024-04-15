package sstable

import (
	"bytes"

	"github.com/chettriyuvraj/leveldb-clone/common"
)

type SSTIterHeap []*SSTableIterator

func (h SSTIterHeap) Len() int { return len(h) }

func (h SSTIterHeap) Less(i, j int) bool { return bytes.Compare(h[i].Key(), h[j].Key()) < 0 }

func (h SSTIterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h SSTIterHeap) Elem(i int) *SSTableIterator {
	return h[i]
}

func (h *SSTIterHeap) Push(x any) { /* Should we push empty iterators? */
	iter := x.(*SSTableIterator)
	*h = append(*h, iter)
}

func (h *SSTIterHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h SSTIterHeap) Peek() common.Iterator {
	if h.Len() == 0 {
		return nil
	}
	return h.Elem(h.Len() - 1)
}
