package db

import (
	"bytes"
	"errors"
	"sort"
)

type Iterator interface {
	// Next moves the iterator to the next key/value pair.
	// It returns false if the iterator is exhausted.
	Next() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key/value pair, or nil if done.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	Value() []byte
}

type DB interface {
	// Get gets the value for the given key. It returns an error if the
	// DB does not contain the key.
	Get(key []byte) (value []byte, err error)

	// Has returns true if the DB contains the given key.
	Has(key []byte) (ret bool, err error)

	// Put sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	Put(key, value []byte) error

	// Delete deletes the value for the given key.
	Delete(key []byte) error

	// RangeScan returns an Iterator (see below) for scanning through all
	// key-value pairs in the given range, ordered by key ascending.
	RangeScan(start, limit []byte) (Iterator, error)
}

var ErrKeyDoesNotExist = errors.New("this key does not exist")
var ErrIdxOutOfBounds = errors.New("index out of bounds")
var ErrInvalidRange = errors.New("range is invalid")

type LevelDBIterator struct {
	*LevelDB
	err error
	idx int
}

type DBEntry struct {
	key, val []byte
}

type LevelDB struct {
	entries []*DBEntry
}

func NewLevelDB() *LevelDB {
	return &LevelDB{entries: []*DBEntry{}}
}

func newDBEntry(key, val []byte) *DBEntry {
	return &DBEntry{key: key, val: val}
}

func (db *LevelDB) Get(key []byte) (val []byte, err error) {
	entry, _, err := db.getDBEntry(key)
	if err != nil {
		return []byte{}, err
	}

	return entry.val, nil
}

/* Wrapper around Get */
func (db *LevelDB) Has(key []byte) (ret bool, err error) {
	_, err = db.Get(key)
	if err != nil {
		if errors.Is(err, ErrKeyDoesNotExist) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (db *LevelDB) Put(key, val []byte) error {
	entry, _, err := db.getDBEntry(key)
	if err != nil {
		if errors.Is(err, ErrKeyDoesNotExist) {
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

func (db *LevelDB) Delete(key []byte) error {
	_, i, err := db.getDBEntry(key)
	if err != nil {
		return err
	}

	db.entries = append(db.entries[:i], db.entries[i+1:]...)
	return nil
}

/* s = k2, k6; k1, k3, k4 , k5, k7 */

func (db *LevelDB) RangeScan(start, limit []byte) (Iterator, error) {
	if bytes.Compare(start, limit) > 0 {
		return nil, ErrInvalidRange
	}

	startIdx, endIdx := 0, len(db.entries)
	startFound, endFound := false, false
	iter := NewLevelDBIterator(NewLevelDB())
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

func (db *LevelDB) getDBEntry(key []byte) (entry *DBEntry, idx int, err error) {
	for i, entry := range db.entries {
		if bytes.Equal(key, entry.key) {
			return entry, i, nil
		}
	}

	return nil, -1, ErrKeyDoesNotExist
}

func (db *LevelDB) getDBEntryByIdx(idx int) (entry *DBEntry, err error) {
	if idx < 0 || idx >= len(db.entries) {
		return nil, ErrIdxOutOfBounds
	}

	return db.entries[idx], nil
}

func NewLevelDBIterator(db *LevelDB) *LevelDBIterator {
	return &LevelDBIterator{LevelDB: db, idx: 0}
}

func (iter *LevelDBIterator) Next() bool {
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

func (iter *LevelDBIterator) Error() error {
	return iter.err
}

func (iter *LevelDBIterator) Key() []byte {
	entry, err := iter.getDBEntryByIdx(iter.idx)
	if err != nil {
		if errors.Is(err, ErrIdxOutOfBounds) {
			return []byte{}
		}
		iter.err = err
		return []byte{}
	}

	return entry.key
}

func (iter *LevelDBIterator) Value() []byte {
	entry, err := iter.getDBEntryByIdx(iter.idx)
	if err != nil {
		if errors.Is(err, ErrIdxOutOfBounds) {
			return []byte{}
		}
		iter.err = err
		return []byte{}
	}

	return entry.val
}

/* Make []DBEntry into interface 'sort.Interface' */
type DBEntrySlice []*DBEntry

func (dbe DBEntrySlice) Len() int           { return len(dbe) }
func (dbe DBEntrySlice) Swap(i, j int)      { dbe[i], dbe[j] = dbe[j], dbe[i] }
func (dbe DBEntrySlice) Less(i, j int) bool { return bytes.Compare(dbe[i].key, dbe[j].key) == -1 }
