package main

import (
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/memdb"
)

/* Workaround done to match signature with test suite */
/* Note: Comment out 'Has' and 'RangeScan' methods from common.DB interface + any references in test suite to run this benchmark*/
func newLevelDBAsInterface() common.DB {
	memdb := memdb.New(comparer.DefaultComparer, 16)
	return memdb
}

func BenchmarkDB(b *testing.B) {
	test.BenchmarkDB(b, test.DBTester{New: newLevelDBAsInterface})
}
