package memdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/sstable"
)

const (
	P                    = 0.25
	MAXLEVEL             = 12
	DEFAULTINDEXDISTANCE = 15
)

const (
	REGULARNODE = iota
	TOMBSTONENODE
)

var ErrEmptyKeyNotAllowed = errors.New("empty key not allowed")

type MemDB struct {
	skiplist.SkipList
	size int /* Sum of sizes of the k-v pairs */
}
type MemDBIterator struct {
	*MemDB
	startKey, limitKey []byte
	curNode            *skiplist.Node
	hasEnded           bool
	skipTombstones     bool
	err                error
}

func (db *MemDB) String() string {
	return db.SkipList.String()
}

func (db *MemDB) Size() int {
	return db.size
}

func NewMemDB() (*MemDB, error) {
	return &MemDB{SkipList: *skiplist.NewSkipList(P, MAXLEVEL)}, nil
}

func (db *MemDB) Get(key []byte) (val []byte, err error) {
	node := db.Search(key)
	if node == nil || bytes.Equal(node.Metadata(), []byte{TOMBSTONENODE}) {
		return nil, common.ErrKeyDoesNotExist
	}
	return node.Val(), nil
}

func (db *MemDB) Has(key []byte) (ret bool, err error) {
	node := db.Search(key)
	if node == nil {
		return false, nil
	}
	return true, nil
}

/* Note: Not allowing empty keys */
func (db *MemDB) Put(key, val []byte) error {
	if bytes.Equal(key, []byte{}) {
		return ErrEmptyKeyNotAllowed
	}

	/* Check if key already exists - this is actually for updating the size of memdb */
	prevVal, err := db.Get(key)
	keyAlreadyExists := true
	if err != nil {
		if !errors.Is(err, common.ErrKeyDoesNotExist) {
			return err
		}
		keyAlreadyExists = false
	}

	if err := db.Insert(key, val, []byte{REGULARNODE}); err != nil {
		return err
	}

	/* Modify db size depending on whether key already existed or not */
	if keyAlreadyExists {
		db.size += len(val) - len(prevVal)
	} else {
		db.size += len(key) + len(val)
	}

	return nil
}

func (db *MemDB) Delete(key []byte) error {
	/* Get value of key if it already exists - we will insert a tombstone only if record exists */
	val, err := db.Get(key)
	if err != nil { /* Return err regardless of whether it is actual error / key does not exist error */
		return err
	}

	/* Delete will insert a tombstone node */
	if err := db.InsertTombstone(key); err != nil {
		return err
	}

	db.size += len(key) + len(val)
	return nil
}

func (db *MemDB) InsertTombstone(key []byte) error {
	if err := db.Insert(key, nil, []byte{TOMBSTONENODE}); err != nil {
		return fmt.Errorf("error inserting tombstone")
	}
	return nil
}

/* Note: limitKey -> nil indicates scan till end of range */
func NewMemDBIterator(db *MemDB, startKey, limitKey []byte, skipTombstones bool) *MemDBIterator {
	iter := MemDBIterator{MemDB: db, startKey: startKey, limitKey: limitKey}

	if bytes.Compare(startKey, limitKey) > 0 && limitKey != nil {
		iter.err = common.ErrInvalidRange
		return &iter
	}

	firstNode := db.SearchClosest(startKey)
	if firstNode == nil {
		iter.hasEnded = true
	} else {
		iter.curNode = firstNode
	}

	return &iter
}

func (db *MemDB) RangeScan(start, limit []byte) (common.Iterator, error) {
	iter := NewMemDBIterator(db, start, limit, true)
	return iter, iter.Error()
}

/* Gives entire data including tombstones */
func (db *MemDB) FullScan() (common.Iterator, error) {
	iter := NewMemDBIterator(db, db.FirstKey(), nil, false)
	return iter, iter.Error()
}

func (db *MemDB) FlushSSTable(f io.Writer) error {
	iter, err := db.FullScan()
	if err != nil {
		return err
	}

	data, err := sstable.GetSSTableData(iter, DEFAULTINDEXDISTANCE)
	if err != nil {
		return fmt.Errorf("error flushing to SSTable: %w", err)
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

/*
- Assuming iter always initialized using NewMemDBIterator func so all constraints defined there hold
*/
func (iter *MemDBIterator) Next() bool {
	iter.err = nil

	if iter.hasEnded {
		return false
	}

	iter.curNode = iter.curNode.GetAdjacent()

	/* limitKey -> nil indicates scan till end of range */
	if iter.limitKey == nil && iter.curNode != nil {
		return true
	}

	if iter.curNode == nil || bytes.Compare(iter.curNode.Key(), iter.limitKey) > 0 {
		iter.curNode = nil
		iter.hasEnded = true
		return false
	}

	if iter.skipTombstones && bytes.Equal(iter.curNode.Metadata(), []byte{TOMBSTONENODE}) {
		return iter.Next()
	}

	return true
}

func (iter *MemDBIterator) Key() []byte {
	iter.err = nil

	if iter.hasEnded || iter.err != nil || iter.curNode == nil {
		return nil
	}
	return iter.curNode.Key()
}

func (iter *MemDBIterator) Value() []byte {
	iter.err = nil

	if iter.hasEnded || iter.err != nil || iter.curNode == nil {
		return nil
	}
	return iter.curNode.Val()
}

func (iter *MemDBIterator) Error() error {
	return iter.err
}
