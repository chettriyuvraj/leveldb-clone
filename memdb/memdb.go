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
	P                  = 0.25
	MAXLEVEL           = 12
	DEFAULTWALFILENAME = "log"
	DEFAULTSSTFILENAME = "sst"
)

var ErrNoSSTableDataToWrite = errors.New("no SSTable data to write")

type MemDB struct {
	skiplist.SkipList
}
type MemDBIterator struct {
	*MemDB
	startKey, limitKey []byte
	curNode            *skiplist.Node
	hasEnded           bool
	err                error
}

type SSTableDirectory struct {
	entries []SSTableDirEntry
}

type SSTableDirEntry struct {
	len    uint32
	key    []byte
	offset uint64
}

func (db *MemDB) String() string {
	return db.SkipList.String()
}

func NewMemDB() (*MemDB, error) {
	return &MemDB{*skiplist.NewSkipList(P, MAXLEVEL)}, nil
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
		return common.ErrKeyDoesNotExist
	}
	if err := db.Insert(key, val); err != nil {
		return err
	}
	return nil
}

func (db *MemDB) Delete(key []byte) error {
	if err := db.SkipList.Delete(key); err != nil { /* Not using embedded skiplist method here directly as it is the same as db method name (Delete) */
		if errors.Is(err, skiplist.ErrKeyDoesNotExist) {
			return common.ErrKeyDoesNotExist
		}
		return err
	}
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

func (db *MemDB) flushSSTable(f io.Writer) error {
	data, err := db.getSSTableData()
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
*/

func (db *MemDB) getSSTableData() (data []byte, err error) {
	iter, err := db.FullScan()
	if err != nil {
		return nil, fmt.Errorf("error getting SSTable: %w", err)
	}

	/* Scan all entries in sorted order + keep track of their offsets + construct SSTable */
	dir := SSTableDirectory{}
	curOffset := 8
	for {
		curData := []byte{}
		k, v := iter.Key(), iter.Value()
		if k == nil {
			break
		}

		dir.entries = append(dir.entries, SSTableDirEntry{key: k, offset: uint64(curOffset)})
		curData = binary.BigEndian.AppendUint32(curData, uint32(len(k)))
		curData = append(curData, k...)
		curData = binary.BigEndian.AppendUint32(curData, uint32(len(v)))
		curOffset += len(curData)
		if len(v) > 0 {
			curData = append(curData, v...)
			curOffset += len(v)
		}
		data = append(data, curData...)

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

	/* Combine directoryOffset:SSTableData:directory */
	data = append(dirOffset, data...)
	data = append(data, dirData...)

	return data, nil

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
	if iter.hasEnded || iter.err != nil || iter.curNode == nil {
		return nil
	}
	return iter.curNode.Key()
}

func (iter *MemDBIterator) Value() []byte {
	if iter.hasEnded || iter.err != nil || iter.curNode == nil {
		return nil
	}
	return iter.curNode.Val()
}

func (iter *MemDBIterator) Error() error {
	return iter.err
}
