package wal

import (
	"errors"
	"io"
	"os"
)

type WAL struct {
	file     io.Writer
	filename string
	fileFlag int
}

type WALFlag byte

var ErrOnlyOnePrimaryModeAllowed = errors.New("only one primary mode can be chosen")
var ErrOnePrimaryModeRequired = errors.New("at least one primary mode must be chosen")
var ErrInvalidPrimaryMode = errors.New("invalid primary mode")

const (
	/* These are primary flags - only 1 of them can be chosen */
	RDONLY = WALFlag(0x01 << iota)
	WRONLY /* Which means only appends */
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

// func (log *WAL) Append(k, v []byte, op byte) error {
// 	if log.fileFlag != file

// 	log, err := NewLogRecord(k, v, op)
// 	if err != nil {
// 		return err
// 	}

// }
