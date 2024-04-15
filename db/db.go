package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/memdb"
	"github.com/chettriyuvraj/leveldb-clone/sstable"
	"github.com/chettriyuvraj/leveldb-clone/wal"
)

const (
	DEFAULTWALFILENAME   = "log"
	DEFAULTSSTFILENAME   = "sst"
	DEFAULTCOMPACTIONDIR = "compact"
	LEVEL0SSTLIMIT       = 4
	LEVEL1SSTFILESIZE    = 80 /* In bytes */
)

type DB struct {
	dirName         string
	memdb           *memdb.MemDB
	memdbLimit      int /* Max size of memdb before flush */
	sstables        []sstable.SSTableDB
	compactSSTables []sstable.SSTableDB
	log             *wal.WAL
}

type DBConfig struct {
	memdbLimit int
	createNew  bool /* Should we create new DB if dirName already exists? */
	dirName    string
}

var ErrMemDB = errors.New("error while querying memdb")
var ErrInitDB = errors.New("error initializing DB")
var ErrWALPUT = errors.New("error appending PUT to WAL")
var ErrWALDELETE = errors.New("error appending DELETE to WAL")
var ErrWALReplay = errors.New("error replaying records from WAL")
var ErrSSTableCreate = errors.New("error creating SSTable file")
var ErrCompactionDB = errors.New("error compacting DB")

func NewDBConfig(memdbLimit int, createNew bool, dirName string) DBConfig {
	return DBConfig{memdbLimit: memdbLimit, createNew: createNew, dirName: dirName}
}

/* Initialize DB only using this function */
func NewDB(config DBConfig) (*DB, error) {
	dirName := config.dirName

	/* Create directory for DB */
	exists, err := fileOrDirExists(dirName)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}
	if exists && config.createNew {
		err := emptyDir(dirName, true)
		if err != nil {
			return nil, err
		}
	}
	if !exists {
		err := os.Mkdir(dirName, 0777)
		if err != nil {
			errors.Join(ErrInitDB, err)
		}
	}

	/* Attach WAL */
	logPath := filepath.Join(dirName, DEFAULTWALFILENAME)
	log, err := wal.Open(logPath)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	/* Attach SSTables if they exist, both current and compacted ones */
	sstables, err := getExistingSSTables(dirName)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}
	compactionDir := filepath.Join(dirName, DEFAULTCOMPACTIONDIR)
	compactSSTables, err := getExistingSSTables(compactionDir)
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	memdb, err := memdb.NewMemDB()
	if err != nil {
		return nil, errors.Join(ErrInitDB, err)
	}

	return &DB{memdb: memdb, log: log, dirName: dirName, memdbLimit: config.memdbLimit, sstables: sstables, compactSSTables: compactSSTables}, nil
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
		return db.searchSSTables(key)
	}

	/* Tombstone encountered in memtable */
	if val == nil {
		return nil, common.ErrKeyDoesNotExist
	}

	return val, nil
}

func (db *DB) searchSSTables(key []byte) (val []byte, err error) {
	tables := append(db.sstables, db.compactSSTables...)
	/* Search each sstable; TODO : search only compacted tables which match the range of the key */
	for _, sst := range tables {
		val, err := sst.Get(key)
		if err != nil {
			if !errors.Is(err, common.ErrKeyDoesNotExist) {
				return nil, fmt.Errorf("error searching sstables: %w", err)
			}
			continue
		}
		/* Tombstone encountered - in SSTables, values of length 0 imply tombstones */
		if val == nil {
			break
		}
		return val, nil
	}

	return nil, common.ErrKeyDoesNotExist
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
	if len(val) == 0 {
		return common.ErrValDoesNotExist
	}

	dataSize := len(key) + len(val)

	/* Check if Put will exceed memdb limit */
	if db.memdb.Size()+dataSize > db.memdbLimit {
		if len(db.sstables) > LEVEL0SSTLIMIT {
			err := db.compact()
			if err != nil {
				return err
			}

			err = db.resetMemDB()
			if err != nil {
				return err
			}

			return db.putToMemDB(key, val)
		}

		err := db.flushToSSTable()
		if err != nil {
			return err
		}

		err = db.resetMemDB()
		if err != nil {
			return err
		}
	}

	return db.putToMemDB(key, val)
}

func (db *DB) putToMemDB(key, val []byte) error {
	if len(val) == 0 {
		return common.ErrValDoesNotExist
	}

	err := db.log.Append(key, val, wal.PUT)
	if err != nil {
		return errors.Join(ErrWALPUT, err)
	}

	if err := db.memdb.Put(key, val); err != nil {
		return errors.Join(ErrMemDB, err)
	}

	return nil
}

func (db *DB) resetMemDB() error {
	/* Truncate log file and seek to the start */
	err := os.Truncate(db.log.Filename(), 0)
	if err != nil {
		return errors.Join(ErrSSTableCreate, err)
	}
	_, err = db.log.Seek(0, 0)
	if err != nil {
		return errors.Join(ErrSSTableCreate, err)
	}

	/* Create new memdb */
	memdb, err := memdb.NewMemDB()
	if err != nil {
		return errors.Join(ErrSSTableCreate, err)
	}
	db.memdb = memdb

	return nil
}

/* Flushes MemDB to SSTable */
func (db *DB) flushToSSTable() error {
	filename, err := getNextSSTableName(db.dirName)
	if err != nil {
		return err
	}

	sstPath := filepath.Join(db.dirName, filename)
	f, err := os.OpenFile(sstPath, os.O_RDWR|os.O_CREATE, 0777) /* TODO: use lesser permissions */
	if err != nil {
		return errors.Join(ErrSSTableCreate, err)
	}
	defer f.Close()

	err = db.memdb.FlushSSTable(f)
	if err != nil {
		return errors.Join(ErrSSTableCreate, err)
	}

	sstable, err := sstable.OpenSSTableDB(sstPath)
	if err != nil {
		return errors.Join(ErrSSTableCreate, err)
	}
	db.sstables = append(db.sstables, sstable)

	return nil
}

func (db *DB) Delete(key []byte) error { // to modify in memdb
	if db.log != nil {
		err := db.log.Append(key, nil, wal.DELETE)
		if err != nil {
			return errors.Join(ErrWALDELETE, err)
		}
	}

	/* Check if key exists */
	if _, err := db.Get(key); err != nil {
		return err
	}

	/* Insert tombstone only if key exists */
	if err := db.memdb.InsertTombstone(key); err != nil {
		return errors.Join(ErrMemDB, err)
	}

	return nil
}

/* TODO: Implement range scans with ss tables */
func (db *DB) RangeScan(start, limit []byte) (common.Iterator, error) {
	return NewMergeIterator(db, start, limit)
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

func (db *DB) compact() error {
	compactionDir := filepath.Join(db.dirName, DEFAULTCOMPACTIONDIR)
	compactionDirTemp := filepath.Join(db.dirName, fmt.Sprintf("%stemp", DEFAULTCOMPACTIONDIR))

	/* Create directory for compaction - temp and then rename later*/
	compactionDirTempExists, err := fileOrDirExists(compactionDirTemp)
	if err != nil {
		return errors.Join(ErrCompactionDB, err)
	}
	if !compactionDirTempExists {
		err := os.Mkdir(compactionDirTemp, 0777)
		if err != nil {
			errors.Join(ErrCompactionDB, err)
		}
	}
	compactionDirExists, err := fileOrDirExists(compactionDir)
	if err != nil {
		return errors.Join(ErrCompactionDB, err)
	}

	/* Add prev compacted sstables to dbs sstable list  */
	prevCompactedSSTables, err := getExistingSSTables(compactionDir)
	if err != nil {
		return errors.Join(ErrCompactionDB, err)
	}
	combinedSSTables := append(db.sstables, prevCompactedSSTables...)

	/* Compute total size of data ~ roughly */
	totalSize := uint64(db.memdb.Size())
	for _, sst := range combinedSSTables {
		totalSize += sst.Size()
	}

	/* Do a full scan on the entire data and split it into equal sized pieces - passing a dummy db obj since actual one used for incoming reads until data fully compacted */
	fullScanIter, err := NewFullMergeIterator(&DB{memdb: db.memdb, sstables: combinedSSTables})
	if err != nil {
		return err
	}

	/* Create compaction files in temp dir, then delete old compaction folder + rename temp dir + delete level 0 sstables */
	if err = db.createCompactionFiles(compactionDirTemp, fullScanIter, LEVEL1SSTFILESIZE); err != nil {
		return errors.Join(ErrCompactionDB, err)
	}

	if compactionDirExists {
		if err := emptyDir(compactionDir, true); err != nil {
			errors.Join(ErrCompactionDB, err)
		}
		if err := os.Remove(compactionDir); err != nil {
			errors.Join(ErrCompactionDB, err)
		}
	}

	if err := os.Rename(compactionDirTemp, compactionDir); err != nil {
		return errors.Join(ErrCompactionDB, err)
	}

	if err = removeSSTFiles(db.dirName); err != nil {
		return errors.Join(ErrCompactionDB, err)
	}

	db.sstables = []sstable.SSTableDB{}
	db.compactSSTables, err = getExistingSSTables(compactionDir)
	if err != nil {
		return errors.Join(ErrCompactionDB, err)
	}

	return nil
}

func (db *DB) createCompactionFiles(compactionDir string, iter common.Iterator, sizePerFile uint64) error {
	for iter.Key() != nil {
		data, err := sstable.GetSSTableDataUntilLimit(iter, sstable.DEFAULTINDEXDISTANCE, sizePerFile)
		if err != nil {
			return errors.Join(ErrCompactionDB, err)
		}

		/* If current level 0 sstables are named as 'sst1', 'sst2' .. these will be 'sst3', 'sst4' for now, then renamed as 'sst1', 'sst2'... */
		filename, err := getNextSSTableName(compactionDir)
		if err != nil {
			return err
		}

		sstPath := filepath.Join(compactionDir, filename)
		f, err := os.OpenFile(sstPath, os.O_RDWR|os.O_CREATE, 0777) /* TODO: use lesser permissions */
		if err != nil {
			return errors.Join(ErrSSTableCreate, err)
		}
		defer f.Close()

		_, err = f.Write(data)
		if err != nil {
			return err
		}

	}

	return nil

}

/* Can we do this differently? */
func (db *DB) Close() error {
	return db.log.Close()
}

/* Gets next SSTableName WRT 'dirName' inside the current directory */
func getNextSSTableName(dirName string) (string, error) {
	dirEntries, err := os.ReadDir(dirName)
	if err != nil {
		return "", err
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

func getExistingSSTables(dirName string) (sstables []sstable.SSTableDB, err error) {
	/* check if dir exists */
	exists, err := fileOrDirExists(dirName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []sstable.SSTableDB{}, nil
	}

	dirEntries, err := os.ReadDir(dirName)
	if err != nil {
		return nil, err
	}

	/* Grab all sst file names - we are sure that any prefix '{DEFAULTSSTFILENAME}' belongs to sst files only */
	sstFileNames := []string{}
	for _, dirEntry := range dirEntries {
		if strings.HasPrefix(dirEntry.Name(), DEFAULTSSTFILENAME) {
			sstFileNames = append(sstFileNames, dirEntry.Name())
		}
	}

	for _, filename := range sstFileNames {
		path := filepath.Join(dirName, filename)
		sst, err := sstable.OpenSSTableDB(path)
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, sst)
	}

	return sstables, nil
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

func emptyDir(dirName string, recurse bool) error {
	dirEntries, err := os.ReadDir(dirName)
	if err != nil {
		return fmt.Errorf("error reading dir entries to empty %w", err)
	}

	for _, dirEntry := range dirEntries {
		dirEntryPath := filepath.Join(dirName, dirEntry.Name())
		info, err := os.Stat(dirEntryPath)
		if err != nil {
			return fmt.Errorf("error emptying dir entries %w", err)
		}

		if info.IsDir() {
			if !recurse {
				continue
			}
			err := emptyDir(dirEntryPath, recurse)
			if err != nil {
				return fmt.Errorf("error emptying dir entries %w", err)
			}
			if err := os.Remove(dirEntryPath); err != nil {
				return fmt.Errorf("error emptying dir entries %w", err)
			}
		} else { /* is a file */
			err := os.Remove(dirEntryPath)
			if err != nil {
				return fmt.Errorf("error emptying dir entries %w", err)
			}
		}
	}
	return nil
}

func removeSSTFiles(dirName string) error {
	dirEntries, err := os.ReadDir(dirName)
	if err != nil {
		return fmt.Errorf("error reading dir entries to empty %w", err)
	}

	for _, dirEntry := range dirEntries {
		dirEntryPath := filepath.Join(dirName, dirEntry.Name())
		info, err := os.Stat(dirEntryPath)
		if err != nil {
			return fmt.Errorf("error removing sst files from dir %s %w", dirName, err)
		}

		if !info.IsDir() && strings.HasPrefix(dirEntry.Name(), DEFAULTSSTFILENAME) {
			err := os.Remove(dirEntryPath)
			if err != nil {
				return fmt.Errorf("error removing sst files from dir %s %w", dirName, err)
			}
		}
	}
	return nil
}
