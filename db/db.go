package db

import (
	"bytes"
	"errors"
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
	entry, err := db.getDBEntry(key)
	if errors.Is(err, ErrKeyDoesNotExist) {
		return []byte{}, ErrKeyDoesNotExist
	}

	return entry.val, nil
}

func (db *LevelDB) Put(key, val []byte) error {
	entry, err := db.getDBEntry(key)
	if errors.Is(err, ErrKeyDoesNotExist) {
		dbEntry := newDBEntry(key, val)
		db.entries = append(db.entries, dbEntry)
		return nil
	}

	entry.val = val
	return nil
}

func (db *LevelDB) getDBEntry(key []byte) (*DBEntry, error) {
	for _, entry := range db.entries {
		if bytes.Equal(key, entry.key) {
			return entry, nil
		}
	}

	return nil, ErrKeyDoesNotExist
}
