package wal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

/* To mock files for test */
type BytesBufferDummySeeker struct {
	bytes.Buffer
}

func (b *BytesBufferDummySeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func TestAppendAndReplay(t *testing.T) {
	records := []LogRecord{
		{key: []byte("k1"), val: nil, op: DELETE},
		{key: []byte("k2"), val: []byte("v2"), op: PUT},
		{key: []byte("k3"), val: nil, op: DELETE},
	}

	/* Append to log and replay the same data successfully */
	tcs := []struct {
		name    string
		records []LogRecord
	}{
		{"append 1 log", records[0:1]},
		{"append multiple logs", records},
	}

	for _, tc := range tcs {
		log := &WAL{file: &BytesBufferDummySeeker{}}
		for _, record := range tc.records {
			err := log.Append(record.key, record.val, record.op)
			require.NoError(t, err)
		}
		replayLogs, err := log.Replay()
		require.NoError(t, err)
		require.Equal(t, tc.records, replayLogs)
	}

}
