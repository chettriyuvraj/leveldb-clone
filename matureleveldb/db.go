package matureleveldb

import (
	"bytes"
	"errors"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
)

const (
	P        = 0.25
	MAXLEVEL = 12
)

type LevelDB struct {
	skiplist.SkipList
}
type LevelDBIterator struct {
	*LevelDB
	startKey, limitKey []byte
	curNode            *skiplist.Node
	hasEnded           bool
	err                error
}

func (db *LevelDB) String() string {
	return db.SkipList.String()
}

func NewLevelDB() *LevelDB {
	return &LevelDB{*skiplist.NewSkipList(P, MAXLEVEL)}
}

func (db *LevelDB) Get(key []byte) (val []byte, err error) {
	node := db.Search(key)
	if node == nil {
		return nil, common.ErrKeyDoesNotExist
	}
	return node.Val(), nil
}

func (db *LevelDB) Has(key []byte) (ret bool, err error) {
	node := db.Search(key)
	if node == nil {
		return false, nil
	}
	return true, nil
}

func (db *LevelDB) Put(key, val []byte) error {
	if err := db.Insert(key, val); err != nil {
		return err
	}
	return nil
}

func (db *LevelDB) Delete(key []byte) error {
	if err := db.SkipList.Delete(key); err != nil { /* Not using embedded skiplist method here directly as it is the same as db method name (Delete) */
		if errors.Is(err, skiplist.ErrKeyDoesNotExist) {
			return common.ErrKeyDoesNotExist
		}
		return err
	}
	return nil
}

func (db *LevelDB) RangeScan(start, limit []byte) (common.Iterator, error) {
	iter := NewLevelDBIterator(db, start, limit)
	return iter, iter.Error()
}

func NewLevelDBIterator(db *LevelDB, startKey, limitKey []byte) *LevelDBIterator {
	iter := LevelDBIterator{LevelDB: db, startKey: startKey, limitKey: limitKey}

	if bytes.Compare(startKey, limitKey) > 0 {
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

/*
- Assuming iter always initialized using NewLevelDBIterator func so all constraints defined there hold
*/
func (iter *LevelDBIterator) Next() bool {
	if iter.hasEnded {
		return false
	}

	iter.curNode = iter.curNode.GetAdjacent()
	if iter.curNode == nil || bytes.Compare(iter.curNode.Key(), iter.limitKey) > 0 {
		iter.curNode = nil
		iter.hasEnded = true
		return false
	}

	return true
}

func (iter *LevelDBIterator) Key() []byte {
	if iter.hasEnded || iter.err != nil || iter.curNode == nil {
		return nil
	}
	return iter.curNode.Key()
}

func (iter *LevelDBIterator) Value() []byte {
	if iter.hasEnded || iter.err != nil || iter.curNode == nil {
		return nil
	}
	return iter.curNode.Val()
}

func (iter *LevelDBIterator) Error() error {
	return iter.err
}
