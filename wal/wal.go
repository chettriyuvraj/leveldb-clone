package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type WAL struct {
	file     io.ReadWriteCloser /* Helpful for things like mocking */
	filename string
	fileFlag int
}

type WALFlag byte

var ErrOnlyOnePrimaryModeAllowed = errors.New("only one primary mode can be chosen")
var ErrOnePrimaryModeRequired = errors.New("at least one primary mode must be chosen")
var ErrInvalidPrimaryMode = errors.New("invalid primary mode")
var ErrFileNotInWRONLYMode = errors.New("file not set to WRONLY mode")
var ErrFileNotInRDONLYMode = errors.New("file not set to RDONLY mode")
var ErrNoUnderlyingFileForLog = errors.New("log does not have any underlying file")

const (
	/* These are primary flags - only 1 of them can be chosen */
	RDONLY = WALFlag(0x01 << iota)
	WRONLY /* NOTE: WRONLY exclusively appends */
	/* These are orModes - may be OR'd to form combinations */
	CREATE
	TRUNC
)

var flagMappings map[WALFlag]int = map[WALFlag]int{
	RDONLY: os.O_RDONLY,
	WRONLY: os.O_WRONLY | os.O_APPEND,
	CREATE: os.O_CREATE,
	TRUNC:  os.O_TRUNC,
}

func Open(filename string, wf WALFlag) (*WAL, error) {
	log := WAL{filename: filename}

	fileFlag, err := WALFlagToFileFlag(wf)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(filename, fileFlag, 0x777) /* TODO: use lesser permissions */
	if err != nil {
		return nil, err
	}
	log.file = f
	log.fileFlag = fileFlag

	return &log, nil
}

func WALFlagToFileFlag(wf WALFlag) (int, error) {
	/* Grab primary and or flags separately */
	primary := wf & 0x03
	or := wf & 0b11111100
	if primary == 0x00 {
		return 0, ErrOnePrimaryModeRequired
	}
	if primary == 0x03 {
		return 0, ErrOnlyOnePrimaryModeAllowed
	}

	/* Convert */
	fileFlag, exists := flagMappings[primary]
	if !exists {
		return 0, ErrInvalidPrimaryMode
	}
	switch {
	case (or & CREATE) != 0:
		fileFlag |= flagMappings[CREATE]
		fallthrough
	case (or & TRUNC) != 0:
		fileFlag |= flagMappings[TRUNC]
	}

	return fileFlag, nil
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

	appendFileFlag, err := WALFlagToFileFlag(WRONLY)
	if err != nil {
		return 0, err
	}
	if log.fileFlag != appendFileFlag {
		return 0, ErrFileNotInWRONLYMode
	}

	return log.file.Write(b)
}

func (log *WAL) Replay() ([]LogRecord, error) {
	if log.file == nil {
		return nil, ErrNoUnderlyingFileForLog
	}

	readFileFlag, err := WALFlagToFileFlag(RDONLY)
	if err != nil {
		return nil, err
	}
	if log.fileFlag != readFileFlag {
		return nil, ErrFileNotInRDONLYMode
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
