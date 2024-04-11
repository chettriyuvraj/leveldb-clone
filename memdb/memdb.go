package memdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
)

const (
	P                    = 0.25
	MAXLEVEL             = 12
	DEFAULTINDEXDISTANCE = 15
)

var ErrEmptyKeyNotAllowed = errors.New("no SSTable data to write")
var ErrNoSSTableDataToWrite = errors.New("no SSTable data to write")

type MemDB struct {
	skiplist.SkipList
	size int /* Sum of sizes of the k-v pairs */
}
type MemDBIterator struct {
	*MemDB
	startKey, limitKey []byte
	curNode            *skiplist.Node
	hasEnded           bool
	err                error
}

type SSTableDirectory struct {
	entries []*SSTableDirEntry
}

type SSTableDirEntry struct {
	len    uint32
	key    []byte
	offset uint64
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
	if node == nil {
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

	/* Check if key already exists */
	prevVal, err := db.Get(key)
	keyAlreadyExists := true
	if err != nil {
		if !errors.Is(err, common.ErrKeyDoesNotExist) {
			return err
		}
		keyAlreadyExists = false
	}

	if err := db.Insert(key, val); err != nil {
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
	/* Get value of key if it already exists */
	val, err := db.Get(key)
	if err != nil { /* Return err regardless of whether it is actual error / key does not exist error */
		return err
	}

	if err := db.SkipList.Delete(key); err != nil { /* Not using embedded skiplist method here directly as it is the same as db method name (Delete) */
		if errors.Is(err, skiplist.ErrKeyDoesNotExist) {
			return common.ErrKeyDoesNotExist
		}
		return err
	}

	db.size -= len(key) + len(val)
	return nil
}

func (db *MemDB) RangeScan(start, limit []byte) (common.Iterator, error) {
	iter := NewMemDBIterator(db, start, limit)
	return iter, iter.Error()
}

func (db *MemDB) FullScan() (common.Iterator, error) {
	iter := NewMemDBIterator(db, db.FirstKey(), nil)
	return iter, iter.Error()
}

func (db *MemDB) FlushSSTable(f io.Writer) error {
	data, err := db.getSSTableData(DEFAULTINDEXDISTANCE)
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
SSTable notes:
-> Directory is the index to offset of each key in the table, so we can find keys without searching the whole table
SSTableFormat:
1. 0-7 bytes: offset to start of 'directory'
2. [start of data] [key_length(4 bytes):key:val_length(4 bytes):val] x Number of keys
3. [start of directory] [key_length(4 bytes):key:key_offset(8 bytes)] x Number of keys
This looks like
[DirectoryOffset]
[Data]
[Directory]
*/

func (db *MemDB) getSSTableData(distBetweenIndexKeys int) (data []byte, err error) {
	iter, err := db.FullScan()
	if err != nil {
		return nil, fmt.Errorf("error getting SSTable: %w", err)
	}

	/* Scan all entries in sorted order + keep track of their offsets + construct SSTable */
	dir := SSTableDirectory{}
	curOffset, curDistanceBetweenKeys := 8, 0
	for {
		k, v := iter.Key(), iter.Value()
		kvSize := len(k) + len(v)
		if k == nil {
			break
		}

		/* Only append entry to index if distance between keys ~ distBetween keys OR key is the first key, since we are creating a sparse index  */
		if len(data) == 0 || curDistanceBetweenKeys+kvSize > distBetweenIndexKeys {
			dirEntry := &SSTableDirEntry{key: k, offset: uint64(curOffset)}
			dir.entries = append(dir.entries, dirEntry)
			curDistanceBetweenKeys = 0
		} else {
			curDistanceBetweenKeys += kvSize
		}

		/* Append data record to data */
		dataRecord := createSSTableDataRecord(k, v)
		curOffset += len(dataRecord)
		data = append(data, dataRecord...)

		if nextExists := iter.Next(); !nextExists {
			break
		}
	}

	if len(data) == 0 {
		return nil, ErrNoSSTableDataToWrite
	}

	/* Note the offset at which directory starts + construct the directory for SSTable  */
	dirOffset := binary.BigEndian.AppendUint64([]byte{}, uint64(curOffset))
	dirData := []byte{}
	for _, entry := range dir.entries {
		keyLen := binary.BigEndian.AppendUint32([]byte{}, uint32(len(entry.key)))
		keyOffset := binary.BigEndian.AppendUint64([]byte{}, entry.offset)
		dirData = append(dirData, keyLen...)
		dirData = append(dirData, entry.key...)
		dirData = append(dirData, keyOffset...)
	}

	/* Combine directoryOffset:SSTableData:directory*/
	data = append(dirOffset, data...)
	data = append(data, dirData...)

	return data, nil

}

/*
Format for a single record: [key_length(4 bytes):key:val_length(4 bytes):val]
*/
func createSSTableDataRecord(k, v []byte) (record []byte) {
	record = binary.BigEndian.AppendUint32(record, uint32(len(k)))
	record = append(record, k...)
	record = binary.BigEndian.AppendUint32(record, uint32(len(v)))
	if len(v) > 0 {
		record = append(record, v...)
	}
	return record
}

/* Note: limitKey -> nil indicates scan till end of range */
func NewMemDBIterator(db *MemDB, startKey, limitKey []byte) *MemDBIterator {
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
