package memdb

import (
	"encoding/binary"
	"errors"
	"io"
)

type SSTableDB struct {
	f   io.ReadWriteCloser
	dir SSTableDirectory
}

var ErrNoSSTableDirOffset = errors.New("no offset for directory in SSTable file")
var ErrInvalidSSTableDirOffset = errors.New("dir offset does not exist in SSTable file")

// func OpenSSTableDB(filename string) (SSTableDB, error) {
// 	f, err := os.Open(filename)
// 	if err != nil {
// 		return SSTableDB{}, nil
// 	}

// 	return SSTableDB{f: f}, nil
// }

/* Note: It will ignore incomplete entries at the end */
func getSSTableDir(SSTableData []byte) (SSTableDirectory, error) {
	if len(SSTableData) < 8 {
		return SSTableDirectory{}, ErrNoSSTableDirOffset
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

	return dir, nil
}
