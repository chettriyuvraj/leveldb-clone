package db

import (
	"bytes"
	"container/heap"
	"errors"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/sstable"
)

var ErrCreateDBIter = errors.New("error creating DB iterator")

type MergeIterator struct {
	startKey, limitKey []byte
	iters              []common.Iterator
	heap               sstable.SSTIterHeap
	memdbIter          common.Iterator
	fullScan           bool
}

func NewFullMergeIterator(db *DB) (*MergeIterator, error) {
	iter := MergeIterator{heap: sstable.SSTIterHeap{}}

	/* Get iterator from memdb */
	memdbIter, err := db.memdb.FullScan()
	if err != nil {
		return nil, errors.Join(ErrCreateDBIter, err)
	}
	if memdbIter.Key() != nil {
		iter.memdbIter = memdbIter
	}

	/* Get iterator from all sstables */
	for _, sst := range db.sstables {
		sstIter, err := sst.FullScan()
		if err != nil {
			return nil, errors.Join(ErrCreateDBIter, err)
		}
		if sstIter.Key() != nil {
			iter.iters = append(iter.iters, sstIter)
		}
	}

	/* Initialize heap */
	heap.Init(&iter.heap)
	for _, validIters := range iter.iters {
		heap.Push(&iter.heap, validIters)
	}

	return &iter, err
}

/*
- Tombstones are to be skipped
- Grab all range-matching sstables iterators + memdb iterator
*/
func NewMergeIterator(db *DB, startKey, limitKey []byte) (*MergeIterator, error) {
	iter := MergeIterator{startKey: startKey, limitKey: limitKey, heap: sstable.SSTIterHeap{}}

	/* Get iterator if it matches range, from memdb */
	memdbIter, err := db.memdb.RangeScan(startKey, limitKey)
	if err != nil {
		return nil, errors.Join(ErrCreateDBIter, err)
	}
	if memdbIter.Key() != nil {
		iter.memdbIter = memdbIter
	}

	/* Get iterator which match range from all sstables */
	for _, sst := range db.sstables {
		sstIter, err := sst.RangeScan(startKey, limitKey)
		if err != nil {
			return nil, errors.Join(ErrCreateDBIter, err)
		}
		if sstIter.Key() != nil {
			iter.iters = append(iter.iters, sstIter)
		}
	}

	/* Initialize heap */
	heap.Init(&iter.heap)
	for _, validIters := range iter.iters {
		heap.Push(&iter.heap, validIters)
	}

	return &iter, err
}

func (iter *MergeIterator) Next() bool {
	minIter := MinIter(iter.memdbIter, iter.heap)

	if minIter == nil {
		return false
	}

	/* Move minIter to next smallest Key */
	if minIter == iter.memdbIter {
		iter.memdbIter.Next()
	} else {
		poppedIter := heap.Pop(&iter.heap).(*sstable.SSTableIterator)
		if poppedIter.Next() {
			heap.Push(&iter.heap, poppedIter)
		}
	}

	/* Check if at least one of the iterators is alive */
	return iter.memdbIter.Key() != nil || iter.heap.Len() > 0
}

func (iter *MergeIterator) Key() []byte {
	minIter := MinIter(iter.memdbIter, iter.heap)

	if minIter == nil {
		return nil
	}

	return minIter.Key()
}

func (iter *MergeIterator) Value() []byte {
	minIter := MinIter(iter.memdbIter, iter.heap)

	if minIter == nil {
		return nil
	}

	return minIter.Value()
}

func (iter *MergeIterator) Error() error {
	minIter := MinIter(iter.memdbIter, iter.heap)

	if minIter == nil {
		return nil
	}

	return minIter.Error()
}

func MinIter(memdbIter common.Iterator, heap sstable.SSTIterHeap) common.Iterator {
	if memdbIter.Key() == nil && heap.Len() == 0 {
		return nil
	}

	var minIter common.Iterator = memdbIter
	if heap.Len() > 0 {
		i1, i2 := memdbIter, heap[0]
		if i1.Key() == nil && i2.Key() == nil {
			return nil
		}

		if i1.Key() == nil {
			minIter = i2
		} else if bytes.Compare(i2.Key(), i1.Key()) < 0 {
			minIter = heap[0]
		}
	}

	return minIter
}
