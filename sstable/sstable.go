package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/chettriyuvraj/leveldb-clone/common"
)

const DEFAULTINDEXDISTANCE = 15

type SSTableDirectory struct {
	entries []*SSTableDirEntry
}

type SSTableDirEntry struct {
	len    uint32
	key    []byte
	offset uint64
}

type SSTableDB struct {
	f         io.ReadSeekCloser
	dir       *SSTableDirectory
	dirOffset uint64
}

/* Iterator will store the current offset for the range, and move to the next one on next + check if we have exceeded limit */
type SSTableIterator struct {
	db             *SSTableDB
	fileOffset     uint64
	endKey         []byte
	fullScan       bool
	curKey, curVal []byte /* After first call to Value() or Key(), this is cached */
	hasEnded       bool
	err            error
}

var ErrNoSSTableDirOffset = errors.New("no offset for directory in SSTable file")
var ErrInvalidSSTableDirOffset = errors.New("dir offset does not exist in SSTable file")
var ErrNewSSTableOpen = errors.New("error opening new SSTable")
var ErrNewSSTableCreate = errors.New("error creating new SSTable")
var ErrSSTableGet = errors.New("error getting data from SSTable")
var ErrNewSSTableIter = errors.New("error creating new SST iterator")
var ErrSSTableIterNext = errors.New("error moving to next item in SSTable iterator")
var ErrNoSSTableDataToWrite = errors.New("no SSTable data to write")

func OpenSSTableDB(filename string) (db SSTableDB, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return db, errors.Join(ErrNewSSTableOpen, err)
	}

	/* TODO: SSTables at higher levels will be larger so read them in a buffered manner instead of all at once */
	return NewSSTableDB(f)
}

func NewSSTableDB(f io.ReadSeekCloser) (db SSTableDB, err error) {
	data, err := io.ReadAll(f)
	if err != nil {
		return db, errors.Join(ErrNewSSTableCreate, err)
	}

	dir, dirOffset, err := getSSTableDir(data)
	if err != nil {
		return db, errors.Join(ErrNewSSTableCreate, err)
	}

	return SSTableDB{f: f, dir: dir, dirOffset: dirOffset}, nil
}

/*
Format specified in README
*/

func GetSSTableData(iter common.Iterator, distBetweenIndexKeys int) (data []byte, err error) {
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

/* Note: It will ignore incomplete entries at the end */
func getSSTableDir(SSTableData []byte) (dir *SSTableDirectory, dirOffset uint64, err error) {
	if len(SSTableData) < 8 {
		return nil, 0, ErrNoSSTableDirOffset
	}

	dirOffset = binary.BigEndian.Uint64(SSTableData[:8])
	curOffset := dirOffset
	dir = &SSTableDirectory{entries: []*SSTableDirEntry{}}
	for {
		if curOffset+4 > uint64(len(SSTableData)) {
			break
		}
		keyLen := binary.BigEndian.Uint32(SSTableData[curOffset : curOffset+4])
		curOffset += 4

		if curOffset+uint64(keyLen) > uint64(len(SSTableData)) {
			break
		}
		key := SSTableData[curOffset : curOffset+uint64(keyLen)]
		curOffset += uint64(keyLen)

		if curOffset+8 > uint64(len(SSTableData)) {
			break
		}
		keyOffset := binary.BigEndian.Uint64(SSTableData[curOffset : curOffset+8])
		curOffset += 8

		dirEntry := SSTableDirEntry{len: keyLen, key: key, offset: keyOffset}
		dir.entries = append(dir.entries, &dirEntry)
	}

	return dir, dirOffset, nil
}

/* Find index of first key greater than or equal to the current key using binary search. Returns 'n' if key does not exist */
func (db *SSTableDB) getRightBisect(key []byte) int {
	entries, entriesN := db.dir.entries, len(db.dir.entries)
	return sort.Search(entriesN, func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})
}

func (db *SSTableDB) Get(key []byte) (value []byte, err error) {
	entries := db.dir.entries

	/* First find left and right bounds using binary search */
	rightBound := db.getRightBisect(key)
	leftBound := rightBound - 1
	if rightBound < len(entries) && bytes.Equal(entries[rightBound].key, key) {
		leftBound = rightBound
	}
	if leftBound < 0 {
		return nil, common.ErrKeyDoesNotExist
	}

	/* Seek to the left bound */
	curOffset := entries[leftBound].offset
	_, err = db.Seek(int64(curOffset), 0)
	if err != nil {
		return nil, errors.Join(ErrSSTableGet)
	}

	/* Now continue searching until we are sure key cannot be found */
	for curOffset < db.dirOffset {
		keyLen := make([]byte, 4)
		_, err = db.f.Read(keyLen)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Join(ErrSSTableGet)
		}

		curKey := make([]byte, binary.BigEndian.Uint32(keyLen))
		_, err = db.f.Read(curKey)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Join(ErrSSTableGet)
		}

		valLen := make([]byte, 4)
		_, err = db.f.Read(valLen)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Join(ErrSSTableGet)
		}

		curVal := make([]byte, binary.BigEndian.Uint32(valLen))
		_, err = db.f.Read(curVal)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Join(ErrSSTableGet)
		}

		if bytes.Equal(key, curKey) {
			return curVal, nil
		}

		if bytes.Compare(key, curKey) < 0 {
			break
		}
	}

	return nil, common.ErrKeyDoesNotExist
}

func (db *SSTableDB) Has(key []byte) (ret bool, err error) {
	_, err = db.Get(key)
	if err != nil {
		if errors.Is(err, common.ErrKeyDoesNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (db *SSTableDB) FullScan() (common.Iterator, error) {
	iter, err := NewFullSSTableIterator(db)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (db *SSTableDB) RangeScan(start, limit []byte) (common.Iterator, error) {
	iter, err := NewSSTableIterator(db, start, limit)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (db *SSTableDB) Seek(offset int64, whence int) (int64, error) {
	originOffset, err := db.f.Seek(offset, whence)
	if err != nil {
		return -1, err
	}
	return originOffset, nil
}

func (db *SSTableDB) Close() error {
	return db.f.Close()
}

func NewSSTableIterator(db *SSTableDB, start, limit []byte) (*SSTableIterator, error) {
	entries, entriesN := db.dir.entries, len(db.dir.entries)

	if bytes.Compare(start, limit) > 0 {
		return nil, common.ErrInvalidRange
	}

	/* Find first key equal to or greater than startKey */
	startIdx := db.getRightBisect(start)
	if startIdx == entriesN {
		return &SSTableIterator{db: db, hasEnded: true, endKey: limit}, nil
	}

	/* Seek to this key  */
	curOffset := entries[startIdx].offset
	_, err := db.Seek(int64(curOffset), 0)
	if err != nil {
		return nil, errors.Join(ErrNewSSTableIter, err)
	}

	/* Add first key, val to iterator */
	keyLen := make([]byte, 4)
	_, err = db.f.Read(keyLen)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, endKey: limit}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += 4

	curKey := make([]byte, binary.BigEndian.Uint32(keyLen))
	_, err = db.f.Read(curKey)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, endKey: limit}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += uint64(len(curKey))

	valLen := make([]byte, 4)
	_, err = db.f.Read(valLen)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, endKey: limit}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += 4

	curVal := make([]byte, binary.BigEndian.Uint32(valLen))
	_, err = db.f.Read(curVal)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, endKey: limit}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += uint64(len(curVal))

	/* No elems found in the range - first key itself exceeds limit */
	if bytes.Compare(curKey, limit) > 0 {
		return &SSTableIterator{db: db, fileOffset: curOffset, endKey: limit, hasEnded: true}, nil
	}

	return &SSTableIterator{db: db, fileOffset: curOffset, endKey: limit, curKey: curKey, curVal: curVal}, nil
}

func NewFullSSTableIterator(db *SSTableDB) (*SSTableIterator, error) {
	entries, entriesN := db.dir.entries, len(db.dir.entries)

	/* No directory entries */
	if entriesN == 0 {
		return &SSTableIterator{hasEnded: true, fullScan: true}, nil
	}

	/* Seek to first key  */
	curOffset := entries[0].offset
	_, err := db.Seek(int64(curOffset), 0)
	if err != nil {
		return nil, errors.Join(ErrNewSSTableIter, err)
	}

	/* Add first key, val to iterator */
	keyLen := make([]byte, 4)
	_, err = db.f.Read(keyLen)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, fullScan: true}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += 4

	curKey := make([]byte, binary.BigEndian.Uint32(keyLen))
	_, err = db.f.Read(curKey)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, fullScan: true}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += uint64(len(curKey))

	valLen := make([]byte, 4)
	_, err = db.f.Read(valLen)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, fullScan: true}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += 4

	curVal := make([]byte, binary.BigEndian.Uint32(valLen))
	_, err = db.f.Read(curVal)
	if err != nil {
		if err == io.EOF {
			return &SSTableIterator{db: db, hasEnded: true, fullScan: true}, nil
		}
		return nil, errors.Join(ErrNewSSTableIter, err)
	}
	curOffset += uint64(len(curVal))

	return &SSTableIterator{db: db, fileOffset: curOffset, fullScan: true, curKey: curKey, curVal: curVal}, nil
}

/*
- If iterator errors out other than for io.EOF, it will remain at the same offset with same k,v
*/
func (iter *SSTableIterator) Next() bool {
	if iter.hasEnded {
		return false
	}

	if iter.fileOffset >= iter.db.dirOffset {
		iter.curKey, iter.curVal = nil, nil
		iter.hasEnded = true
		return false
	}

	db := iter.db
	offset := iter.fileOffset

	/* Add next key, val to iterator */
	keyLen := make([]byte, 4)
	_, err := db.f.Read(keyLen)
	if err != nil {
		if err == io.EOF {
			iter.curKey, iter.curVal = nil, nil
			iter.hasEnded = true
			return false
		}
		iter.err = err
		return false
	}
	offset += 4

	curKey := make([]byte, binary.BigEndian.Uint32(keyLen))
	_, err = db.f.Read(curKey)
	if err != nil {
		if err == io.EOF {
			iter.curKey, iter.curVal = nil, nil
			iter.hasEnded = true
			return false
		}
		iter.err = err
		return false
	}
	offset += uint64(len(curKey))

	valLen := make([]byte, 4)
	_, err = db.f.Read(valLen)
	if err != nil {
		if err == io.EOF {
			iter.curKey, iter.curVal = nil, nil
			iter.hasEnded = true
			return false
		}
		iter.err = err
		return false
	}
	offset += 4

	curVal := make([]byte, binary.BigEndian.Uint32(valLen))
	_, err = db.f.Read(curVal)
	if err != nil {
		if err == io.EOF {
			iter.curKey, iter.curVal = nil, nil
			iter.hasEnded = true
			return false
		}
		iter.err = err
		return false
	}
	offset += uint64(len(curVal))

	/* Check if range limit exceeded - unless we are full scanning*/
	if !iter.fullScan && bytes.Compare(curKey, iter.endKey) > 0 {
		iter.curKey, iter.curVal = nil, nil
		iter.hasEnded = true
		return false
	}

	iter.curKey = curKey
	iter.curVal = curVal
	iter.err = nil
	iter.fileOffset = offset

	return true
}

func (iter *SSTableIterator) Key() []byte {
	return iter.curKey
}

/*
- We use Get which searches for the offset again, even though we already have the offset with us, can be improved
*/
func (iter *SSTableIterator) Value() []byte {
	return iter.curVal
}

func (iter *SSTableIterator) Error() error {
	return iter.err
}
