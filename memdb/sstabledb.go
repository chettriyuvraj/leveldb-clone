package memdb

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

type SSTableDB struct {
	f   io.ReadSeekCloser
	dir *SSTableDirectory
}

var ErrNoSSTableDirOffset = errors.New("no offset for directory in SSTable file")
var ErrInvalidSSTableDirOffset = errors.New("dir offset does not exist in SSTable file")

func OpenSSTableDB(filename string) (db SSTableDB, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return db, fmt.Errorf("error opening SSTable: %w", err)
	}

	/* TODO: SSTables at higher levels will be larger so read them in a buffered manner instead of all at once */
	data, err := io.ReadAll(f)
	if err != nil {
		return db, fmt.Errorf("error opening SSTable: %w", err)
	}

	dir, err := getSSTableDir(data)
	if err != nil {
		return db, fmt.Errorf("error opening SSTable: %w", err)
	}

	return SSTableDB{f: f, dir: dir}, nil
}

/* Note: It will ignore incomplete entries at the end */
func getSSTableDir(SSTableData []byte) (*SSTableDirectory, error) {
	if len(SSTableData) < 8 {
		return nil, ErrNoSSTableDirOffset
	}

	dirOffset := binary.BigEndian.Uint64(SSTableData[:8])
	curOffset := dirOffset
	dir := SSTableDirectory{entries: []SSTableDirEntry{}}
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
		dir.entries = append(dir.entries, dirEntry)
	}

	return &dir, nil
}

func (db *SSTableDB) Get(key []byte) (value []byte, err error) {
	/* First find offset of key in directory using binary search */
	i, found := sort.Find(len(db.dir.entries), func(i int) int {
		return bytes.Compare(key, db.dir.entries[i].key)
	})
	if !found {
		return nil, common.ErrKeyDoesNotExist
	}

	/* Query the SSTable file by seeking to the offset of the key and reading it's value */
	dirEntry := db.dir.entries[i]
	keyLen := len(dirEntry.key)
	_, err = db.Seek(int64(dirEntry.offset)+4+int64(keyLen), 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking data in SSTable: %w", err)
	}
	valLen := make([]byte, 4)
	_, err = db.f.Read(valLen)
	if err != nil {
		return nil, fmt.Errorf("error reading val length in SSTable: %w", err)
	}
	val := make([]byte, binary.BigEndian.Uint32(valLen))
	_, err = db.f.Read(val)
	if err != nil {
		return nil, fmt.Errorf("error reading val in SSTable: %w", err)
	}

	return val, nil
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
