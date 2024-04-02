package wal

/*
1 individual log record format:
- 1 byte op-type
- 4 byte key length
- {key-length} bytes key
- 4 byte val length
- {val-length} bytes val
As little endian
*/

import (
	"encoding/binary"
	"errors"
)

const (
	PUT = byte(iota)
	DELETE
)

const MINIMUMLOGSIZE = 9

var opmap map[byte]bool = map[byte]bool{
	PUT:    true,
	DELETE: true,
}

var ErrOpDoesNotExist = errors.New("the provided op does not exist")
var ErrMinLogSize = errors.New("size of log lesser than the minimum log size")
var ErrKeySmallerThanKeyLen = errors.New("size of key lesser than key length specified")
var ErrNoValDataExists = errors.New("binary log record ends after key")
var ErrValSmallerThanValLen = errors.New("size of val lesser than val length specified")

type LogRecord struct {
	key, val []byte
	op       byte
}

func NewLogRecord(k, v []byte, op byte) LogRecord {
	return LogRecord{key: k, val: v}
}

func (log *LogRecord) MarshalBinary() (data []byte, err error) {
	data = []byte{log.op}
	data = binary.BigEndian.AppendUint32(data, uint32(len(log.key)))
	data = append(data, log.key...)
	data = binary.BigEndian.AppendUint32(data, uint32(len(log.val)))
	data = append(data, log.val...)
	return data, nil
}

func (log *LogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < MINIMUMLOGSIZE {
		return ErrMinLogSize
	}

	bytesRead := 0

	/* Read op */
	op := data[0]
	if _, exists := opmap[op]; !exists {
		return ErrOpDoesNotExist
	}
	log.op = op
	bytesRead += 1

	/* Read key len */
	kLen := data[1:5]
	bytesRead += 4

	/* Read key */
	kStart, kEnd := 5, int(5+binary.BigEndian.Uint32(kLen))
	if kEnd-kStart > len(data)-bytesRead {
		return ErrKeySmallerThanKeyLen
	}
	log.key = data[kStart:kEnd]
	bytesRead += kEnd - kStart

	/* Read val len*/
	vLenStart, vLenEnd := kEnd, kEnd+4
	if len(data) < vLenEnd {
		return ErrNoValDataExists
	}
	vLen := data[vLenStart:vLenEnd]
	bytesRead += vLenEnd - vLenStart

	/* Read val */
	vStart, vEnd := vLenEnd, vLenEnd+int(binary.BigEndian.Uint32(vLen))
	if vEnd-vStart > len(data)-bytesRead {
		return ErrValSmallerThanValLen
	}
	log.val = data[vStart:vEnd]
	bytesRead += vEnd - vStart

	return nil

}
