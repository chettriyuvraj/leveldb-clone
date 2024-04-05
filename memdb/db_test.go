package memdb

import (
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/test"
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
