package matureleveldb

import (
	"fmt"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/skiplist"
)

func TestPackageImport(t *testing.T) {
	sl := skiplist.NewSkipList(0.3, 1)
	fmt.Println(*sl)
}
