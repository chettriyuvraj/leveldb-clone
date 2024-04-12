package memdb

import (
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* Workaround done exclusively to match signature with test suite */
func newMemDBAsInterface() common.DB {
	return &MemDB{*skiplist.NewSkipList(P, MAXLEVEL), 0}
}

func newMemDBIteratorAsInterface(db common.DB) common.Iterator {
	return &MemDBIterator{MemDB: db.(*MemDB)}
}

func TestDB(t *testing.T) {
	test.TestDB(t, test.DBTester{New: newMemDBAsInterface})
	// test.TestIterator(t, test.IteratorTester{New: newMemDBIteratorAsInterface}, test.DBTester{New: newMemDBAsInterface}) /* This test is not valid as a stand-alone as iteratators are coupled to rangescan in this implementation */
}

func BenchmarkDB(b *testing.B) {
	test.BenchmarkDB(b, test.DBTester{New: newMemDBAsInterface})
}

/* Implementation-specific tests */

func TestFullScan(t *testing.T) {
	/* Test empty memdb */
	db, err := NewMemDB()
	require.NoError(t, err)
	iter, err := db.FullScan()
	require.NoError(t, err)
	test.IteratorTestNext(t, iter, false, false)
	test.IteratorTestKey(t, iter, nil, false)
	test.IteratorTestVal(t, iter, nil, false)

	/* Test non-empty db */
	tcs := []struct {
		k, v        []byte
		isTombstone bool
	}{
		{k: []byte("key1"), v: []byte("val1")},
		{k: []byte("key2"), v: []byte("val2")},
		{k: []byte("key3"), v: []byte("val3"), isTombstone: true},
		{k: []byte("key4"), v: []byte("val4"), isTombstone: true},
		{k: []byte("key5"), v: []byte("val5")},
	}

	for i := range tcs {
		records := tcs[:i+1]
		db, err := NewMemDB()
		require.NoError(t, err)

		/* Populate a subset of the test case records */
		for _, record := range records {
			err := db.Put(record.k, record.v)
			require.NoError(t, err)
			if record.isTombstone {
				err := db.Delete(record.k)
				require.NoError(t, err)
			}
		}

		/* Verify if we can get entire subset using FullScan() */
		iter, err = db.FullScan()
		require.NoError(t, err)
		for j := 0; j < i+1; j++ {
			recordExpected := tcs[j]
			keyExpected, valExpected := recordExpected.k, recordExpected.v
			test.IteratorTestKey(t, iter, keyExpected, false)
			if recordExpected.isTombstone {
				test.IteratorTestVal(t, iter, nil, false)
			} else {
				test.IteratorTestVal(t, iter, valExpected, false)
			}
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
