package db

import (
	"errors"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/memdb"
	"github.com/chettriyuvraj/leveldb-clone/wal"
)

const (
	DEFAULTWALFILENAME = "log"
)

type DB struct {
	memdb *memdb.MemDB
	log   *wal.WAL
}

var ErrMemDB = errors.New("error while querying memdb")
var ErrInitDB = errors.New("error initializing DB")
var ErrWALPUT = errors.New("error appending PUT to WAL")
var ErrWALDELETE = errors.New("error appending DELETE to WAL")
var ErrWALReplay = errors.New("error replaying records from WAL")

/* Initialize DB only using this function */
func NewDB() (*DB, error) {
	log, err := wal.Open(DEFAULTWALFILENAME)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	memdb, err := memdb.NewMemDB()
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	return &DB{memdb: memdb, log: log}, nil
}

/* DB is attached with a default WAL, but we have the option to attach our own as well */
func (db *DB) AttachWAL(filename string) error {
	log, err := wal.Open(filename)
	if err != nil {
		return err
	}
	db.log = log
	return nil
}

func (db *DB) Get(key []byte) (val []byte, err error) {
	val, err = db.memdb.Get(key)
	if err != nil {
		if !errors.Is(err, common.ErrKeyDoesNotExist) {
			return nil, errors.Join(ErrMemDB, err)
		}
		return nil, err
	}
	return val, nil
}

func (db *DB) Has(key []byte) (ret bool, err error) {
	_, err = db.Get(key)
	if err != nil {
		if !errors.Is(err, common.ErrKeyDoesNotExist) {
			return false, errors.Join(ErrMemDB, err)
		}
		return false, nil
	}
	return true, nil
}

func (db *DB) Put(key, val []byte) error { // to modify in memdb
	err := db.log.Append(key, val, wal.PUT)
	if err != nil {
		return errors.Join(ErrWALPUT, err)
	}

	if err := db.memdb.Put(key, val); err != nil {
		return errors.Join(ErrMemDB, err)
	}
	return nil
}

func (db *DB) Delete(key []byte) error { // to modify in memdb
	if db.log != nil {
		err := db.log.Append(key, nil, wal.DELETE)
		if err != nil {
			return errors.Join(ErrWALDELETE, err)
		}
	}

	if err := db.memdb.Delete(key); err != nil {
		return errors.Join(ErrMemDB, err)
	}

	return nil
}

func (db *DB) RangeScan(start, limit []byte) (common.Iterator, error) {
	iter := memdb.NewMemDBIterator(db.memdb, start, limit)
	return iter, iter.Error()
}

func (db *DB) Replay() error {
	records, err := db.log.Replay()
	if err != nil {
		return errors.Join(ErrWALReplay, err)
	}
	for _, record := range records {
		op := record.Op()
		switch op {
		case wal.PUT:
			err := db.Put(record.Key(), record.Val())
			if err != nil {
				return errors.Join(ErrWALReplay, ErrWALPUT, err)
			}
		case wal.DELETE:
			err := db.Delete(record.Key())
			if err != nil {
				return errors.Join(ErrWALReplay, ErrWALDELETE, err)
			}
		}
	}
	return nil
}

func (db *DB) Close() error {
	return db.log.Close()
}

// type DB interface {

// 	// Get gets the value for the given key. It returns an error if the
// 	// DB does not contain the key.
// 	Get(key []byte) (value []byte, err error)

// 	// Has returns true if the DB contains the given key.
// 	Has(key []byte) (ret bool, err error)

// 	// Put sets the value for the given key. It overwrites any previous value
// 	// for that key; a DB is not a multi-map.
// 	Put(key, value []byte) error

// 	// Delete deletes the value for the given key.
// 	Delete(key []byte) error

// 	// RangeScan returns an Iterator (see below) for scanning through all
// 	// key-value pairs in the given range, ordered by key ascending.
// 	RangeScan(start, limit []byte) (Iterator, error)
// }
