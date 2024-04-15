package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

var TESTDBCONFIG DBConfig = DBConfig{
	dirName:    "testDB",
	memdbLimit: 10,
	createNew:  true, /* Each test will create a new db */
}

/* Workaround done exclusively to match signature with test suite */
func NewDBAsInterface() common.DB {
	db, _ := NewDB(TESTDBCONFIG)
	return db
}

func cleanupTestDB(t *testing.T) {
	exists, err := fileOrDirExists(TESTDBCONFIG.dirName)
	require.NoError(t, err)
	if exists {
		emptyDir(TESTDBCONFIG.dirName, true) /* Assuming it contains only files - TODO - delete compaction folder as well */
		err := os.Remove(TESTDBCONFIG.dirName)
		require.NoError(t, err)
	}
}

func TestDB(t *testing.T) {
	defer cleanupTestDB(t)
	test.TestDB(t, test.DBTester{New: NewDBAsInterface})
}

func BenchmarkDB(b *testing.B) {
	defer cleanupTestDB(&testing.T{})
	test.BenchmarkDB(b, test.DBTester{New: NewDBAsInterface})
}

/* Implementation specific tests */
func TestWAL(t *testing.T) {
	defer cleanupTestDB(t)

	TESTWALCONFIG := DBConfig{
		dirName:    TESTDBCONFIG.dirName,
		memdbLimit: 50,    /* Make memdb large enough to fit all data */
		createNew:  false, /* Don't create new ss tables / db files */
	}

	const (
		DELETE = iota
		PUT
	)

	records := []struct {
		k, v []byte
		op   int
	}{
		{k: []byte("key1"), v: []byte("val1"), op: PUT},
		{k: []byte("key2"), v: []byte("val2"), op: PUT},
		{k: []byte("key3"), v: []byte("val3"), op: PUT},
		{k: []byte("key4"), v: []byte("val4"), op: PUT},
		{k: []byte("key4"), op: DELETE},
		{k: []byte("key3"), op: DELETE},
	}

	/* Init db1 and populate */
	db1, err := NewDB(TESTWALCONFIG)
	require.NoError(t, err)
	defer db1.Close()

	for _, record := range records {
		switch record.op {
		case PUT:
			err := db1.Put(record.k, record.v)
			require.NoError(t, err)
		case DELETE:
			err := db1.Delete(record.k)
			require.NoError(t, err)
		}
	}

	/* Use the same WAL + retain SSTables */
	db2, err := NewDB(TESTWALCONFIG)
	require.NoError(t, err)
	defer db2.Close()
	err = db2.Replay()
	require.NoError(t, err)

	tcs := []struct {
		k, v   []byte
		exists bool
	}{
		{k: []byte("key1"), v: []byte("val1"), exists: true},
		{k: []byte("key2"), v: []byte("val2"), exists: true},
		{k: []byte("key3"), exists: false},
		{k: []byte("key4"), exists: false},
	}

	/* Check if contents are the same as db1 */
	for _, tc := range tcs {
		switch tc.exists {
		case true:
			v, err := db2.Get(tc.k)
			require.Equal(t, tc.v, v) /* Note: No repeated keys in records PUT */
			require.NoError(t, err)
		case false:
			v, err := db2.Get(tc.k)
			require.Equal(t, []byte(nil), v) /* Note: DELETES always after PUT in records so keys don't reappear */
			require.Error(t, err, common.ErrKeyDoesNotExist)
		}
	}
}

func TestGetNextSSTableName(t *testing.T) {
	_, err := NewDB(TESTDBCONFIG)
	require.NoError(t, err)
	defer cleanupTestDB(t)
	for i := 1; i <= 5; i++ {
		/* Check if next sst filename for "test" directory correct - since dir is empty we should get "sst1", "sst2"...in order after creating each one */
		filenameWant := fmt.Sprintf("%s%d", DEFAULTSSTFILENAME, i)
		filenameGot, err := getNextSSTableName(TESTDBCONFIG.dirName)
		require.NoError(t, err)
		require.Equal(t, filenameWant, filenameGot)

		/* Create the sst filename */
		sstPath := filepath.Join(TESTDBCONFIG.dirName, filenameWant)
		_, err = os.Create(sstPath)
		require.NoError(t, err)
		defer os.Remove(sstPath)
	}
}

// func TestSSTCompaction(t *testing.T) {
// 	TESTCOMPACTIONCONFIG := DBConfig{
// 		dirName:    TESTDBCONFIG.dirName,
// 		memdbLimit: 13,    /* Make mem large enough to fit SOME data => level 0 SSTs will be of this size */
// 		createNew:  false, /* Don't create new ss tables / db files */
// 	}

// 	const (
// 		DELETE = iota
// 		PUT
// 	)

// 	records := []struct {
// 		k, v []byte
// 		op   int
// 	}{
// 		{k: []byte("key1"), v: []byte("val1"), op: PUT},
// 		{k: []byte("key2"), v: []byte("val2"), op: PUT},
// 		{k: []byte("key3"), v: []byte("val3"), op: PUT},
// 		{k: []byte("key4"), v: []byte("val4"), op: PUT},
// 		{k: []byte("key5"), v: []byte("val5"), op: PUT},
// 		{k: []byte("key6"), v: []byte("val6"), op: PUT},
// 		{k: []byte("key7"), v: []byte("val7"), op: PUT},
// 		{k: []byte("key8"), v: []byte("val8"), op: PUT},
// 		{k: []byte("key9"), v: []byte("val9"), op: PUT},
// 		{k: []byte("key10"), v: []byte("val10"), op: PUT},
// 		{k: []byte("key11"), v: []byte("val11"), op: PUT},
// 		{k: []byte("key12"), v: []byte("val12"), op: PUT},
// 		{k: []byte("key4"), op: DELETE},
// 		{k: []byte("key3"), op: DELETE},
// 	}

// 	/* Init db1 and populate */
// 	db1, err := NewDB(TESTCOMPACTIONCONFIG)
// 	require.NoError(t, err)
// 	defer cleanupTestDB(t)
// 	defer db1.Close()

// 	for _, record := range records {
// 		switch record.op {
// 		case PUT:
// 			err := db1.Put(record.k, record.v)
// 			require.NoError(t, err)
// 		case DELETE:
// 			err := db1.Delete(record.k)
// 			require.NoError(t, err)
// 		}
// 	}

// 	tcs := []struct {
// 		k, v   []byte
// 		exists bool
// 	}{
// 		{k: []byte("key1"), v: []byte("val1"), exists: true},
// 		{k: []byte("key2"), v: []byte("val2"), exists: true},
// 		{k: []byte("key3"), v: []byte("val3"), exists: false},
// 		{k: []byte("key4"), v: []byte("val4"), exists: false},
// 		{k: []byte("key5"), v: []byte("val5"), exists: true},
// 		{k: []byte("key6"), v: []byte("val6"), exists: true},
// 		{k: []byte("key7"), v: []byte("val7"), exists: true},
// 		{k: []byte("key8"), v: []byte("val8"), exists: true},
// 		{k: []byte("key9"), v: []byte("val9"), exists: true},
// 		{k: []byte("key10"), v: []byte("val10"), exists: true},
// 		{k: []byte("key11"), v: []byte("val11"), exists: true},
// 		{k: []byte("key12"), v: []byte("val12"), exists: true},
// 	}

// 	/* Check if contents match up to the operations we performed */
// 	for _, tc := range tcs {
// 		switch tc.exists {
// 		case true:
// 			v, err := db1.Get(tc.k)
// 			require.Equal(t, tc.v, v) /* Note: No repeated keys in records PUT */
// 			require.NoError(t, err)
// 		case false:
// 			v, err := db1.Get(tc.k)
// 			require.Equal(t, []byte(nil), v) /* Note: DELETES always after PUT in records so keys don't reappear */
// 			require.Error(t, err, common.ErrKeyDoesNotExist)
// 		}
// 	}

// }
