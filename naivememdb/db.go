package naivememdb

import (
	"bytes"
	"errors"
	"sort"

	"github.com/chettriyuvraj/leveldb-clone/common"
)

type MemDBIterator struct {
	*MemDB
	err error
	idx int
}

type DBEntry struct {
	key, val []byte
}

type MemDB struct {
	entries []*DBEntry
}

func NewMemDB() *MemDB {
	return &MemDB{entries: []*DBEntry{}}
}

func newDBEntry(key, val []byte) *DBEntry {
	return &DBEntry{key: key, val: val}
}

func (db *MemDB) Get(key []byte) (val []byte, err error) {
	entry, _, err := db.get(key)
	if err != nil {
		return nil, err
	}

	return entry.val, nil
}

/* Wrapper around Get */
func (db *MemDB) Has(key []byte) (ret bool, err error) {
	_, err = db.Get(key)
	if err != nil {
		if errors.Is(err, common.ErrKeyDoesNotExist) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (db *MemDB) Put(key, val []byte) error {
	entry, _, err := db.get(key)
	if err != nil {
		if errors.Is(err, common.ErrKeyDoesNotExist) {
			dbEntry := newDBEntry(key, val)
			db.entries = append(db.entries, dbEntry)
			sort.Sort(DBEntrySlice(db.entries))
			return nil
		}
		return err
	}

	entry.val = val
	return nil
}

func (db *MemDB) Delete(key []byte) error {
	_, i, err := db.get(key)
	if err != nil {
		return err
	}

	db.entries = append(db.entries[:i], db.entries[i+1:]...)
	return nil
}

func (db *MemDB) RangeScan(start, limit []byte) (common.Iterator, error) {
	if bytes.Compare(start, limit) > 0 {
		return nil, common.ErrInvalidRange
	}

	startIdx, endIdx := 0, len(db.entries)
	startFound, endFound := false, false
	iter := NewMemDBIterator(NewMemDB())
	for i := 0; i < len(db.entries) && !(startFound && endFound); i++ {

		if !startFound {
			cmp := bytes.Compare(start, db.entries[i].key)
			if cmp == 0 || cmp < 0 {
				startIdx = i
				startFound = true
			}
		} else {
			cmp := bytes.Compare(limit, db.entries[i].key)
			if cmp == 0 {
				endIdx = i + 1
				endFound = true
			} else if cmp < 0 {
				endIdx = i
				endFound = true
			}
		}
	}

	if startFound {
		iter.entries = db.entries[startIdx:endIdx]
	}
	return iter, nil
}

func (db *MemDB) get(key []byte) (entry *DBEntry, idx int, err error) {
	for i, entry := range db.entries {
		if bytes.Equal(key, entry.key) {
			return entry, i, nil
		}
	}

	return nil, -1, common.ErrKeyDoesNotExist
}

func (db *MemDB) getDBEntryByIdx(idx int) (entry *DBEntry, err error) {
	if idx < 0 || idx >= len(db.entries) {
		return nil, common.ErrIdxOutOfBounds
	}

	return db.entries[idx], nil
}

/* TODO: change this to return an error (for consistentcy with other iter interfaces) */
func NewMemDBIterator(db *MemDB) *MemDBIterator {
	return &MemDBIterator{MemDB: db, idx: 0}
}

func (iter *MemDBIterator) Next() bool {
	if iter.idx >= len(iter.entries) {
		return false
	}

	iter.idx++
	/* If we moved from last index to out of bounds */
	if iter.idx == len(iter.entries) {
		return false
	}
	return true
}

func (iter *MemDBIterator) Error() error {
	return iter.err
}

func (iter *MemDBIterator) Key() []byte {
	entry, err := iter.getDBEntryByIdx(iter.idx)
	if err != nil {
		if errors.Is(err, common.ErrIdxOutOfBounds) {
			return nil
		}
		iter.err = err
		return nil
	}

	return entry.key
}

func (iter *MemDBIterator) Value() []byte {
	entry, err := iter.getDBEntryByIdx(iter.idx)
	if err != nil {
		if errors.Is(err, common.ErrIdxOutOfBounds) {
			return nil
		}
		iter.err = err
		return nil
	}

	return entry.val
}

/* Make []DBEntry into interface 'sort.Interface' */
type DBEntrySlice []*DBEntry

func (dbe DBEntrySlice) Len() int           { return len(dbe) }
func (dbe DBEntrySlice) Swap(i, j int)      { dbe[i], dbe[j] = dbe[j], dbe[i] }
func (dbe DBEntrySlice) Less(i, j int) bool { return bytes.Compare(dbe[i].key, dbe[j].key) == -1 }
