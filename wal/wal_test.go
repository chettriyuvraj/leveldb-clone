package wal

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

/* To mock files for test */
type BytesBufferCloser struct {
	bytes.Buffer
}

func (b *BytesBufferCloser) Close() error {
	return nil
}

/* Test flags + errors for Open function */
func TestOpen(t *testing.T) {
	tcs := []struct {
		name             string
		flag             WALFlag
		expectedFileFlag int
		err              error
	}{
		{name: "error: both RDONLY and APPENDONLY chosen", err: ErrOnlyOnePrimaryModeAllowed, flag: 0x03},
		{name: "error: no file modeschosen", err: ErrOnlyOnePrimaryModeAllowed, flag: 0x00},
		{name: "error: invalid file mode", err: ErrOnlyOnePrimaryModeAllowed, flag: 0x04},
		/* Test that WALFlag sets the correct os.file flag */
		{name: "RDONLY flag set", flag: RDONLY, expectedFileFlag: os.O_RDONLY},
		{name: "WRONLY flag set", flag: WRONLY, expectedFileFlag: os.O_WRONLY | os.O_APPEND},
		{name: "RDONLY + CREATE flag set", flag: RDONLY | CREATE, expectedFileFlag: os.O_RDONLY | os.O_CREATE},
		{name: "WRONLY + TRUNC flag set", flag: WRONLY | TRUNC, expectedFileFlag: os.O_WRONLY | os.O_APPEND | os.O_TRUNC},
	}

	for _, tc := range tcs {
		log, err := Open("test", tc.flag)
		if err != nil {
			require.Error(t, err, tc.err)
		} else {
			require.Equal(t, tc.expectedFileFlag, log.fileFlag)
		}
	}
}

func TestAppendAndReplay(t *testing.T) {
	log := &WAL{fileFlag: os.O_RDONLY, file: &BytesBufferCloser{}}
	records := []LogRecord{
		{key: []byte("k1"), val: []byte("v1"), op: DELETE},
		{key: []byte("k2"), val: []byte("v2"), op: PUT},
		{key: []byte("k3"), val: []byte("v3"), op: DELETE},
	}

	/* Test error: log not in WRONLY mode on append */
	r1 := records[0]
	err := log.Append(r1.key, r1.val, r1.op)
	require.Error(t, err, ErrFileNotInWRONLYMode)

	/* Test error: log not in RDONLY mode on append */
	log.fileFlag = flagMappings[WRONLY]
	_, err = log.Replay()
	require.Error(t, err, ErrFileNotInRDONLYMode)

	/* Append to log and replay the same data successfully */
	tcs := []struct {
		name    string
		records []LogRecord
	}{
		{"append 1 log", records[0:1]},
		{"append multiple logs", records},
	}

	for _, tc := range tcs {
		log.fileFlag = flagMappings[WRONLY]
		for _, record := range tc.records {
			err = log.Append(record.key, record.val, record.op)
			require.NoError(t, err)
		}
		log.fileFlag = flagMappings[RDONLY]
		replayLogs, err := log.Replay()
		require.NoError(t, err)
		require.Equal(t, tc.records, replayLogs)
	}

}
