package memdb

import (
	"fmt"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* Workaround done exclusively to match signature with test suite */
func newMemDBAsInterface() common.DB {
	return &MemDB{*skiplist.NewSkipList(P, MAXLEVEL)}
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
	db, err := NewMemDB()
	require.NoError(t, err)

	/* Test empty memdb */
	iter, err := db.FullScan()
	require.NoError(t, err)
	test.IteratorTestNext(t, iter, false, false)
	test.IteratorTestKey(t, iter, nil, false)
	test.IteratorTestVal(t, iter, nil, false)

	/* Populate db */
	for i := 1; i <= 9; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Check if all values obtained using FullScan + no error and nil when exhausted */
	iter, err = db.FullScan()
	require.NoError(t, err)
	for i := 1; i <= 9; i++ {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		test.IteratorTestKey(t, iter, keyExpected, false)
		test.IteratorTestVal(t, iter, valExpected, false)
		if i < 9 {
			test.IteratorTestNext(t, iter, true, false)
		} else {
			test.IteratorTestNext(t, iter, false, false)
			test.IteratorTestKey(t, iter, nil, false)
			test.IteratorTestVal(t, iter, nil, false)
		}
	}

}
