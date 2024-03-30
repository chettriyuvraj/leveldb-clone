package matureleveldb

import (
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/test"
)

/* Workaround done exclusively to match signature with test suite */
func newLevelDBAsInterface() common.DB {
	return &LevelDB{*skiplist.NewSkipList(P, MAXLEVEL)}
}

func newLevelDBIteratorAsInterface(db common.DB) common.Iterator {
	return &LevelDBIterator{LevelDB: db.(*LevelDB)}
}

func TestDB(t *testing.T) {
	test.TestDB(t, test.DBTester{New: newLevelDBAsInterface})
	// test.TestIterator(t, test.IteratorTester{New: newLevelDBIteratorAsInterface}, test.DBTester{New: newLevelDBAsInterface})
}

func BenchmarkDB(b *testing.B) {
	test.BenchmarkDB(b, test.DBTester{New: newLevelDBAsInterface})
}
