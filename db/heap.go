package db

import (
	"bytes"
)

type Record struct {
	key, value []byte
}

type RecordHeap []*Record

func (h RecordHeap) Len() int { return len(h) }

func (h RecordHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) < 0 }

func (h RecordHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h RecordHeap) Elem(i int) Record {
	return *h[i]
}

func (h *RecordHeap) Push(x any) {
	record := x.(Record)
	*h = append(*h, &record)
}

func (h *RecordHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
