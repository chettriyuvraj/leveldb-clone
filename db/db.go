package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/memdb"
	"github.com/chettriyuvraj/leveldb-clone/wal"
)

const (
	DEFAULTWALFILENAME = "log"
	DEFAULTSSTFILENAME = "sst"
	MEMDBLIMIT         = 2000000 /* 2MB - counting only the sizes of keys and vals, not the associated metadata */
)

type DB struct {
	dirName string
	memdb   *memdb.MemDB
	log     *wal.WAL
	memSize int /* Size of memdb, only counting the k:v pairs */
}

var ErrMemDB = errors.New("error while querying memdb")
var ErrInitDB = errors.New("error initializing DB")
var ErrWALPUT = errors.New("error appending PUT to WAL")
var ErrWALDELETE = errors.New("error appending DELETE to WAL")
var ErrWALReplay = errors.New("error replaying records from WAL")
var ErrSSTableCreate = errors.New("error creaeting SSTable file")

/* Initialize DB only using this function */
func NewDB(dirName string) (*DB, error) {
	/* Create directory for DB */
	exists, err := fileOrDirExists(dirName)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}
	if !exists {
		err := os.Mkdir(dirName, 0777)
		if err != nil {
			errors.Join(ErrInitDB, err)
		}
	}

	logPath := filepath.Join(dirName, DEFAULTWALFILENAME)
	log, err := wal.Open(logPath)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	memdb, err := memdb.NewMemDB()
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	return &DB{memdb: memdb, log: log, dirName: dirName}, nil
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
	dataSize := len(key) + len(val)

	/* Check if Put will exceed memdb limit */
	if db.memSize+dataSize > MEMDBLIMIT {
		/* Flush to SSTable */
		filename, err := getNextSSTableName(db.dirName)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777) /* TODO: use lesser permissions */
		if err != nil {
			return errors.Join(ErrSSTableCreate, err)
		}
		defer f.Close()

		err = db.memdb.FlushSSTable(f)
		if err != nil {
			return errors.Join(ErrSSTableCreate, err)
		}

		/* Truncate log file */
		logPath := filepath.Join(db.dirName, db.log.Filename())
		err = os.Truncate(logPath, 0)
		if err != nil {
			return errors.Join(ErrSSTableCreate, err)
		}

		/* Create new memdb */
		memdb, err := memdb.NewMemDB()
		if err != nil {
			return errors.Join(ErrSSTableCreate, err)
		}
		db.memdb = memdb
	}

	err := db.log.Append(key, val, wal.PUT)
	if err != nil {
		return errors.Join(ErrWALPUT, err)
	}

	if err := db.memdb.Put(key, val); err != nil {
		return errors.Join(ErrMemDB, err)
	}

	db.memSize += dataSize
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

/* Gets next SSTableName WRT 'dirName' inside the current directory */
func getNextSSTableName(dirName string) (string, error) {
	dirEntries, err := os.ReadDir(dirName)
	if err != nil {
		log.Fatal(err)
	}

	/* Grab all sst file names - we are sure that any 'DEFAULTSSTFILENAME' belongs to sst files only */
	sstFileNames := []string{}
	for _, dirEntry := range dirEntries {
		if strings.HasPrefix(dirEntry.Name(), DEFAULTSSTFILENAME) {
			sstFileNames = append(sstFileNames, dirEntry.Name())
		}
	}

	curSSTFileIdx := len(sstFileNames) + 1
	return fmt.Sprintf("%s%d", DEFAULTSSTFILENAME, curSSTFileIdx), nil
}

func fileOrDirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
