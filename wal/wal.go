package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type WAL struct {
	file     io.ReadWriteCloser
	filename string
}

var ErrNoUnderlyingFileForLog = errors.New("log does not have any underlying file")

func Open(filename string) (*WAL, error) {
	log := WAL{filename: filename}

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777) /* TODO: use lesser permissions */
	if err != nil {
		return nil, err
	}
	log.file = f

	return &log, nil
}

func (log *WAL) Append(k, v []byte, op byte) error {
	if log.file == nil {
		return ErrNoUnderlyingFileForLog
	}

	record, err := NewLogRecord(k, v, op)
	if err != nil {
		return err
	}

	data, err := record.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = log.Write(data)
	if err != nil {
		return err
	}

	return nil
}

/*
- Don't use by itself, use only through Append() function
*/
func (log *WAL) Write(b []byte) (n int, err error) {
	if log.file == nil {
		return 0, ErrNoUnderlyingFileForLog
	}

	return log.file.Write(b)
}

func (log *WAL) Replay() ([]LogRecord, error) {
	if log.file == nil {
		return nil, ErrNoUnderlyingFileForLog
	}

	records := []LogRecord{}
	for {
		record := LogRecord{}
		bytesRead := 0

		/* Read op */
		op := make([]byte, 1)
		if _, err := log.file.Read(op); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return records, err
		}
		if _, exists := opmap[op[0]]; !exists {
			return records, ErrOpDoesNotExist
		}
		record.op = op[0]
		bytesRead += 1

		/* Read key len */
		kLen := make([]byte, 4)
		if _, err := log.file.Read(kLen); err != nil {
			if errors.Is(err, io.EOF) {
				return records, ErrNoKeyLength
			}
			return records, err
		}
		bytesRead += 4

		/* Read key */
		key := make([]byte, binary.BigEndian.Uint32(kLen))
		if _, err := log.file.Read(key); err != nil {
			if errors.Is(err, io.EOF) {
				return records, ErrNoKeyData
			}
			return records, err
		}
		record.key = key
		bytesRead += int(binary.BigEndian.Uint32(kLen))

		/* Read val len*/
		vLen := make([]byte, 4)
		if _, err := log.file.Read(vLen); err != nil {
			if errors.Is(err, io.EOF) {
				return records, ErrNoValLength
			}
			return records, err
		}
		bytesRead += 4

		if binary.BigEndian.Uint32(vLen) == 0 {
			records = append(records, record)
			continue
		}

		/* Read val */
		val := make([]byte, binary.BigEndian.Uint32(vLen))
		if _, err := log.file.Read(val); err != nil {
			if errors.Is(err, io.EOF) {
				return records, ErrNoValData
			}
			return records, err
		}
		record.val = val
		bytesRead += int(binary.BigEndian.Uint32(vLen))

		records = append(records, record)
	}

	return records, nil
}

func (log *WAL) Close() error {
	if log.file == nil {
		return ErrNoUnderlyingFileForLog
	}
	return log.file.Close()
}
