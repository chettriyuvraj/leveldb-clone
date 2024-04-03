package wal

import (
	"encoding/binary"
	"errors"
)

/*
1 individual log record format:
- 1 byte op-type
- 4 byte key length
- {key-length} bytes key
- 4 byte val length
- {val-length} bytes val
As little endian
*/

const (
	PUT = byte(iota)
	DELETE
)

const (
	MINIMUMRECORDSIZE  = 9
	WALDEFAULTFILENAME = "wal.log"
)

var opmap map[byte]bool = map[byte]bool{
	PUT:    true,
	DELETE: true,
}

var ErrOpDoesNotExist = errors.New("the provided op does not exist")
var ErrMinRecordSize = errors.New("size of record lesser than the minimum record size")
var ErrNoKeyLength = errors.New("no key length exists after op")
var ErrNoKeyData = errors.New("key data of the specified key length does not exist after key length")
var ErrNoValLength = errors.New("no val length exists after key")
var ErrNoValData = errors.New("val data of the specified key length does not exist after val length")

type LogRecord struct {
	key, val []byte
	op       byte
}

func NewLogRecord(k, v []byte, op byte) (*LogRecord, error) {
	if exists := opmap[op]; !exists {
		return nil, ErrOpDoesNotExist
	}
	return &LogRecord{key: k, val: v, op: op}, nil
}

func (record *LogRecord) Op() byte {
	return record.op
}

func (record *LogRecord) Key() []byte {
	return record.key
}

func (record *LogRecord) Val() []byte {
	return record.val
}

func (record *LogRecord) MarshalBinary() (data []byte, err error) {
	data = []byte{record.op}
	data = binary.BigEndian.AppendUint32(data, uint32(len(record.key)))
	data = append(data, record.key...)
	data = binary.BigEndian.AppendUint32(data, uint32(len(record.val)))
	if len(record.val) > 0 {
		data = append(data, record.val...)
	}
	return data, nil
}

func (record *LogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < MINIMUMRECORDSIZE {
		return ErrMinRecordSize
	}

	bytesRead := 0

	/* Read op */
	op := data[0]
	if _, exists := opmap[op]; !exists {
		return ErrOpDoesNotExist
	}
	record.op = op
	bytesRead += 1

	/* Read key len */
	kLen := data[1:5]
	bytesRead += 4

	/* Read key */
	kStart, kEnd := 5, int(5+binary.BigEndian.Uint32(kLen))
	if kEnd-kStart > len(data)-bytesRead {
		return ErrNoKeyData
	}
	record.key = data[kStart:kEnd]
	bytesRead += kEnd - kStart

	/* Read val len*/
	vLenStart, vLenEnd := kEnd, kEnd+4
	if len(data) < vLenEnd {
		return ErrNoValData
	}
	vLen := data[vLenStart:vLenEnd]
	bytesRead += vLenEnd - vLenStart

	/* Read val */
	vStart, vEnd := vLenEnd, vLenEnd+int(binary.BigEndian.Uint32(vLen))
	if vEnd-vStart > len(data)-bytesRead {
		return ErrNoValData
	}
	record.val = data[vStart:vEnd]
	bytesRead += vEnd - vStart

	return nil
}
