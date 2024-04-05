package memdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* Workaround done exclusively to match signature with test suite */
func newMemDBAsInterface() common.DB {
	return &MemDB{*skiplist.NewSkipList(P, MAXLEVEL), nil}
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

/* Implementation specific tests */
func TestWAL(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	wd = fmt.Sprintf("%s/%s", wd, DEFAULTWALFILENAME)

	/* Init db1 and attach a WAL */
	db1, err := NewMemDB()
	require.NoError(t, err)
	err = db1.AttachWAL(wd)
	require.NoError(t, err)
	defer os.Remove(wd)
	defer db1.Close()

	/* Perform ops on db1 to fill WAL */
	for i := 0; i <= 4; i++ {
		k, v := []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))
		err := db1.Put(k, v)
		require.NoError(t, err)
	}
	db1.Delete([]byte("k3"))
	db1.Delete([]byte("k4"))

	/* Use the same WAL to populate db2 */
	db2, err := NewMemDB()
	require.NoError(t, err)
	defer db2.Close()
	err = db2.Replay(wd)
	require.NoError(t, err)

	/* Check if contents are the same as db1 */
	for i := 0; i <= 4; i++ {
		k, vWant := []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))
		vGot, err := db2.Get(k)
		if i < 3 {
			require.NoError(t, err)
			require.Equal(t, vWant, vGot)
		} else {
			require.Error(t, err, common.ErrKeyDoesNotExist)
		}
	}
}
