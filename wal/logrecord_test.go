package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordMarshal(t *testing.T) {
	k, v, op := []byte("key4344"), []byte("val334"), DELETE
	record, err := NewLogRecord(k, v, op)
	require.NoError(t, err)
	data, err := record.MarshalBinary()

	/* Compute expected result by hand and then compare */
	e1 := append([]byte{DELETE, 0x00, 0x00, 0x00, 0x07}, k...)
	e2 := append([]byte{0x00, 0x00, 0x00, 0x06}, v...)
	expected := append(e1, e2...)
	require.NoError(t, err)
	require.Equal(t, expected, data)
}

func TestRecordUnmarshal(t *testing.T) {
	k, v, op := []byte("key4344"), []byte("val334"), DELETE
	record, err := NewLogRecord(k, v, op)
	require.NoError(t, err)
	data, err := record.MarshalBinary()
	require.NoError(t, err)

	tcs := []struct {
		name string
		data []byte
		err  error
		want *LogRecord
	}{
		{name: "unmarshal a simple binary log record", want: record, data: data},
		{name: "violates minimum log size", data: []byte{DELETE, 0x01, 0x02}, err: ErrMinRecordSize},
		{name: "key smaller than key length", data: []byte{DELETE, 0x00, 0x00, 0x00, 0x08, 0x01, 0x02, 0x02, 0x03, 0x04}, err: ErrKeySmallerThanKeyLen},
		{name: "no val data exists", data: []byte{DELETE, 0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x08}, err: ErrNoValData},
		{name: "val smaller than val length", data: []byte{DELETE, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01}, err: ErrValSmallerThanValLen},
	}

	for _, tc := range tcs {
		got := &LogRecord{}
		require.Equal(t, tc.err, got.UnmarshalBinary(tc.data))
		if tc.err == nil {
			require.Equal(t, got, tc.want)
		}
	}
}
