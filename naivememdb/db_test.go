package naivememdb

import (
	"fmt"
	"sort"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* Workaround done exclusively to match signature with test suite */
func newMemDBAsInterface() common.DB {
	return &MemDB{entries: []*DBEntry{}}
}

func newMemDBIteratorAsInterface(db common.DB) common.Iterator {
	return &MemDBIterator{MemDB: db.(*MemDB), idx: 0}
}

func TestDB(t *testing.T) {
	test.TestDB(t, test.DBTester{New: newMemDBAsInterface})
	test.TestIterator(t, test.IteratorTester{New: newMemDBIteratorAsInterface}, test.DBTester{New: newMemDBAsInterface})
}

func BenchmarkDB(b *testing.B) {
	test.BenchmarkDB(b, test.DBTester{New: newMemDBAsInterface})
}

/* This is an implementation-specific test, hence not exported to global test suite */
func TestPutSorted(t *testing.T) {
	db := NewMemDB()
	iterations := 10
	for i := iterations; i >= 0; i-- {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
		require.True(t, sort.IsSorted(DBEntrySlice(db.entries)))
	}
}
