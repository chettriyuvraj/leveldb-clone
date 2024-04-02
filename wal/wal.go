package wal

import (
	"errors"
	"io"
	"os"
)

type WAL struct {
	file     io.Writer
	filename string
	fileFlag int /* Only for book-keeping */
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

var WALFlagToFileFlag map[WALFlag]int = map[WALFlag]int{
	RDONLY: os.O_RDONLY,
	WRONLY: os.O_WRONLY | os.O_APPEND,
	CREATE: os.O_CREATE,
	TRUNC:  os.O_TRUNC,
}

func Open(filename string, flag WALFlag) (*WAL, error) {
	log := WAL{filename: filename}

	/* Grab different flags in separate variables */
	primaryFlag := flag & 0x03
	orFlag := flag & 0b11111100
	if primaryFlag == 0x00 {
		return nil, ErrOnePrimaryModeRequired
	}
	if primaryFlag == 0x03 {
		return nil, ErrOnlyOnePrimaryModeAllowed
	}

	/* Set file flag wrt corresponding WALFlag */
	fileFlag, exists := WALFlagToFileFlag[primaryFlag]
	if !exists {
		return nil, ErrInvalidPrimaryMode
	}
	switch {
	case (orFlag & CREATE) != 0:
		fileFlag |= WALFlagToFileFlag[CREATE]
		fallthrough
	case (orFlag & TRUNC) != 0:
		fileFlag |= WALFlagToFileFlag[TRUNC]
	}

	f, err := os.OpenFile(filename, fileFlag, 0x777) /* TODO: use lesser permissions */
	if err != nil {
		return nil, err
	}
	log.file = f
	log.fileFlag = fileFlag

	return &log, nil

}
