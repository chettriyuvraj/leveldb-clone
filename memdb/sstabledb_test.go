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

func TestGetSSTableDir(t *testing.T) {
	/* Test complete directory data */
	_, SSTableData, expectedDir := dummySSTableData()
	gotDir, err := getSSTableDir(SSTableData)
	require.NoError(t, err)
	require.Equal(t, expectedDir, gotDir)

	/* Test incomplete directory data - any incomplete data will be ignored */
	gotDir, err = getSSTableDir(SSTableData[:len(SSTableData)-3])
	expectedDir.entries = expectedDir.entries[:1]
	require.NoError(t, err)
	require.Equal(t, expectedDir, gotDir)
}

func TestSSTableGet(t *testing.T) {
	/* Mock SSTableDB to test */
	records, SSTableData, dir := dummySSTableData()
	reader := BytesReadWriteSeekCloser{bytes.NewReader(SSTableData)}
	db := SSTableDB{f: reader, dir: dir}

	/* Check if each record found */
	for i := len(records) - 1; i >= 0; i-- {
		k, expectedV := records[i].k, records[i].v
		gotV, err := db.Get(k)
		require.NoError(t, err)
		require.Equal(t, expectedV, gotV)
	}

	/* Check for non-existent record */
	_, err := db.Get([]byte("randomVal"))
	require.ErrorIs(t, err, common.ErrKeyDoesNotExist)
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
	err = memdb.flushSSTable(&b)
	require.NoError(t, err)
	sstData, err := io.ReadAll(b)
	require.NoError(t, err)
	dir, err := getSSTableDir(sstData)
	require.NoError(t, err)
	sstdb := SSTableDB{f: b, dir: dir}

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
