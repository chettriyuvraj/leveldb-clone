package sstable

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* Structs for testing SSTable */

type kvRecord struct{ k, v []byte }

type BytesReadWriteSeekCloser struct {
	*bytes.Reader
}

type DummyIterator struct {
	i       int
	records []kvRecord
}

func (iter *DummyIterator) Next() bool {
	if iter.i < len(iter.records) {
		iter.i++
	}
	return iter.i < len(iter.records)
}

func (iter *DummyIterator) Key() []byte {
	if iter.i < len(iter.records) {
		return iter.records[iter.i].k
	}
	return nil
}

func (iter *DummyIterator) Value() []byte {
	if iter.i < len(iter.records) {
		return iter.records[iter.i].v
	}
	return nil
}

func (iter *DummyIterator) Error() error {
	return nil
}

func (iter *DummyIterator) Reset() {
	iter.i = 0
}

func NewDummyIterator(records []kvRecord) *DummyIterator {
	return &DummyIterator{records: records}
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

/* Implementation-specific tests */
/* Can be more robust, but for now we are testing our ss table generation solely by comparing the directory generated */
func TestGetSSTableDir(t *testing.T) {
	/* Populate DB */
	distBetweenIndexKeys := 10
	records := []kvRecord{
		{[]byte("comp"), []byte("c")},
		{[]byte("extc"), []byte{}},
		{[]byte("mecha"), []byte("mechanical")},
		{[]byte("zebr"), []byte("?")},
	}

	/* Get SSTable data for DB, convert it to dir and compare to expected dir */
	iter := NewDummyIterator(records)
	sstData, err := GetSSTableData(iter, distBetweenIndexKeys)
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
	/* Populate db */
	distBetweenIndexKeys := 10
	records := []kvRecord{
		{[]byte("biot"), []byte("b")},
		{[]byte("comp"), []byte("c")},
		{[]byte("elec"), []byte("e")},
		{[]byte("extc"), []byte{}},
		{[]byte("mecha"), []byte("mechanical")},
		{[]byte("zebr"), []byte("?")},
	}

	/* Get SSTable */
	iter := NewDummyIterator(records)
	sstData, err := GetSSTableData(iter, distBetweenIndexKeys)
	require.NoError(t, err)
	sstDB, err := NewSSTableDB(BytesReadWriteSeekCloser{bytes.NewReader(sstData)})
	require.NoError(t, err)

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
	records := []kvRecord{
		{[]byte("key1"), []byte("val1")},
		{[]byte("key3"), []byte("val3")},
		{[]byte("key5"), []byte("val5")},
		{[]byte("key7"), []byte("val7")},
		{[]byte("key9"), []byte("val9")},
	}

	iter := NewDummyIterator(records)
	sstData, err := GetSSTableData(iter, DEFAULTINDEXDISTANCE)
	require.NoError(t, err)
	sstdb, err := NewSSTableDB(BytesReadWriteSeekCloser{bytes.NewReader(sstData)})
	require.NoError(t, err)

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

func TestFullScan(t *testing.T) {
	records := []kvRecord{
		{[]byte("key1"), []byte("val1")},
		{[]byte("key2"), []byte("val2")},
		{[]byte("key3"), []byte("val3")},
		{[]byte("key4"), []byte("val4")},
		{[]byte("key5"), []byte("val5")},
	}

	for i := range records {
		/* Populate a subset of the test case records */
		curRecords := records[:i+1]
		dummyIter := NewDummyIterator(curRecords)
		sstData, err := GetSSTableData(dummyIter, DEFAULTINDEXDISTANCE)
		require.NoError(t, err)
		db, err := NewSSTableDB(BytesReadWriteSeekCloser{bytes.NewReader(sstData)})
		require.NoError(t, err)

		/* Verify if we can get entire subset using FullScan() */
		iter, err := db.FullScan()
		require.NoError(t, err)
		for j := 0; j < i+1; j++ {
			recordExpected := curRecords[j]
			keyExpected, valExpected := recordExpected.k, recordExpected.v
			test.IteratorTestKey(t, iter, keyExpected, false)
			test.IteratorTestVal(t, iter, valExpected, false)

			if j < i {
				test.IteratorTestNext(t, iter, true, false)
			}
		}

		/* Verify if iterator exhaused after all elems output-ed */
		test.IteratorTestNext(t, iter, false, false)
		test.IteratorTestKey(t, iter, nil, false)
		test.IteratorTestVal(t, iter, nil, false)
	}
}
