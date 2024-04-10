package memdb

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* For testing SSTable */
type BytesReadWriteSeekCloser struct {
	*bytes.Reader
}

func (b *BytesReadWriteSeekCloser) Write(p []byte) (n int, err error) {
	curData, err := io.ReadAll(b)
	if err != nil {
		return 0, err
	}

	updatedData := append(curData, p...)
	b.Reader = bytes.NewReader(updatedData)
	return len(p), nil
}

func (b BytesReadWriteSeekCloser) Close() error {
	return nil
}

/* Can be more robust, but for now we are testing our ss table generation solely by comparing the directory generated */
func TestGetSSTableDir(t *testing.T) {
	/* Populate DB */
	distBetweenIndexKeys := 10
	records := []struct{ k, v []byte }{
		{[]byte("comp"), []byte("c")},
		{[]byte("extc"), []byte{}},
		{[]byte("mecha"), []byte("mechanical")},
		{[]byte("zebr"), []byte("?")},
	}
	db := memDBWithRecords(t, records)

	/* Get SSTable data for DB, convert it to dir and compare to expected dir */
	sstData, err := db.getSSTableData(distBetweenIndexKeys)
	require.NoError(t, err)
	gotDir, _, err := getSSTableDir(sstData)
	require.NoError(t, err)
	expectedDir := SSTableDirectory{
		entries: []*SSTableDirEntry{
			{len: 4, key: records[0].k, offset: 8},
			{len: 5, key: records[2].k, offset: 8 + 16 + 9},
		},
	}

	require.Equal(t, expectedDir, *gotDir)
}

func TestSSTableGet(t *testing.T) {
	/* Populate memdb */
	distBetweenIndexKeys := 10
	records := []struct{ k, v []byte }{
		{[]byte("biot"), []byte("b")},
		{[]byte("comp"), []byte("c")},
		{[]byte("elec"), []byte("e")},
		{[]byte("extc"), []byte{}},
		{[]byte("mecha"), []byte("mechanical")},
		{[]byte("zebr"), []byte("?")},
	}
	db := memDBWithRecords(t, records)

	/* Get SSTable */
	sstData, err := db.getSSTableData(distBetweenIndexKeys)
	require.NoError(t, err)
	sstDir, dirOffset, err := getSSTableDir(sstData)
	require.NoError(t, err)
	sstDB := SSTableDB{f: BytesReadWriteSeekCloser{bytes.NewReader(sstData)}, dir: sstDir, dirOffset: dirOffset}

	/* Check if each record found */
	tcs := []struct {
		name    string
		record  struct{ k, v []byte }
		errWant error
	}{
		{name: "get first record", record: records[0]},
		{name: "get last record", record: records[5]},
		{name: "get last dir indexed record", record: records[4]},
		{name: "get non dir indexed record", record: records[2]},
		{name: "get non-existent record lesser than first key", record: struct {
			k []byte
			v []byte
		}{[]byte("alexa"), []byte("amazon")}, errWant: common.ErrKeyDoesNotExist},
		{name: "get non-existent record lesser than last key", record: struct {
			k []byte
			v []byte
		}{[]byte("zx"), []byte("zx")}, errWant: common.ErrKeyDoesNotExist},
	}

	for _, tc := range tcs {
		v, errGot := sstDB.Get(tc.record.k)
		if tc.errWant != nil {
			require.ErrorIs(t, errGot, tc.errWant)
		} else {
			require.NoError(t, errGot)
			require.Equal(t, tc.record.v, v)
		}
	}
}

func TestSSTableRangeScan(t *testing.T) {
	memdb, err := NewMemDB()
	require.NoError(t, err)

	/* Populate db */
	for i := 1; i <= 9; i += 2 {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := memdb.Put(k, v)
		require.NoError(t, err)
	}

	/* Flush to an SSTable + get data as bytes + create in-mem sstable */
	b := BytesReadWriteSeekCloser{bytes.NewReader([]byte{})}
	err = memdb.FlushSSTable(&b)
	require.NoError(t, err)
	sstData, err := io.ReadAll(b)
	require.NoError(t, err)
	dir, dirOffset, err := getSSTableDir(sstData)
	require.NoError(t, err)
	sstdb := SSTableDB{f: b, dir: dir, dirOffset: dirOffset}

	/* Check exact ranges + confirm if values exhausted afterwards */
	start, end := []byte("key1"), []byte("key9")
	iterator, err := sstdb.RangeScan(start, end)
	require.NoError(t, err)
	for i := 1; i <= 9; i += 2 {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		test.IteratorTestKey(t, iterator, keyExpected, false)
		test.IteratorTestVal(t, iterator, valExpected, false)
		if i < 9 {
			test.IteratorTestNext(t, iterator, true, false)
		} else {
			test.IteratorTestNext(t, iterator, false, false)
		}
	}
	test.IteratorTestNext(t, iterator, false, false)
	test.IteratorTestKey(t, iterator, nil, false)
	test.IteratorTestVal(t, iterator, nil, false)

	/* Check inexact ranges + confirm if values exhausted afterwards */
	start, end = []byte("key"), []byte("key8")
	iterator, err = sstdb.RangeScan(start, end)
	require.NoError(t, err)
	for i := 1; i <= 7; i += 2 {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		test.IteratorTestKey(t, iterator, keyExpected, false)
		test.IteratorTestVal(t, iterator, valExpected, false)
		if i < 7 {
			test.IteratorTestNext(t, iterator, true, false)
		} else {
			test.IteratorTestNext(t, iterator, false, false)
		}
	}
	test.IteratorTestNext(t, iterator, false, false)
	test.IteratorTestKey(t, iterator, nil, false)
	test.IteratorTestVal(t, iterator, nil, false)
}

func memDBWithRecords(t *testing.T, records []struct{ k, v []byte }) *MemDB {
	t.Helper()
	db, err := NewMemDB()
	require.NoError(t, err)
	for _, record := range records {
		err := db.Put(record.k, record.v)
		require.NoError(t, err)
	}
	return db
}
