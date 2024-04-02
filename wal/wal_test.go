package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWALMarshal(t *testing.T) {
	k, v, op := []byte("key4344"), []byte("val334"), PUT
	log := NewLogRecord(k, v, op)
	data, err := log.MarshalBinary()

	/* Compute expected result by hand and then compare */
	e1 := append([]byte{PUT, 0x00, 0x00, 0x00, 0x07}, k...)
	e2 := append([]byte{0x00, 0x00, 0x00, 0x06}, v...)
	expected := append(e1, e2...)
	require.NoError(t, err)
	require.Equal(t, expected, data)
}

func TestWALUnmarshal(t *testing.T) {
	k, v, op := []byte("key4344"), []byte("val334"), PUT
	log := NewLogRecord(k, v, op)
	data, err := log.MarshalBinary()
	require.NoError(t, err)

	tcs := []struct {
		name string
		data []byte
		err  error
		want LogRecord
	}{
		{name: "unmarshal a simple binary log record", want: log, data: data},
		{name: "violates minimum log size", data: []byte{PUT, 0x01, 0x02}, err: ErrMinLogSize},
		{name: "key smaller than key length", data: []byte{PUT, 0x00, 0x00, 0x00, 0x08, 0x01, 0x02, 0x02, 0x03, 0x04}, err: ErrKeySmallerThanKeyLen},
		{name: "no val data exists", data: []byte{DELETE, 0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x08}, err: ErrNoValDataExists},
		{name: "val smaller than val length", data: []byte{DELETE, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01}, err: ErrValSmallerThanValLen},
	}

	for _, tc := range tcs {
		got := LogRecord{}
		require.Equal(t, tc.err, got.UnmarshalBinary(tc.data))
		if tc.err == nil {
			require.Equal(t, got, tc.want)
		}
	}
}
