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

const TESTDBDIR = "testDB"

/* Workaround done exclusively to match signature with test suite */
func NewDBAsInterface() common.DB {
	db, _ := NewDB(TESTDBDIR)
	return db
}

func cleanupTestDB(t *testing.T) {
	logPath := filepath.Join(TESTDBDIR, DEFAULTWALFILENAME)
	exists, err := fileOrDirExists(logPath)
	require.NoError(t, err)
	if exists {
		err := os.Remove(logPath)
		require.NoError(t, err)
	}

	exists, err = fileOrDirExists(TESTDBDIR)
	require.NoError(t, err)
	if exists {
		err := os.Remove(TESTDBDIR)
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

	const (
		DELETE = iota
		PUT
	)

	records := []struct {
		k, v []byte
		op   int
	}{
		{k: []byte("key1"), v: []byte("val1"), op: PUT},
		{k: []byte("key2"), v: []byte("val3"), op: PUT},
		{k: []byte("key3"), v: []byte("val3"), op: PUT},
		{k: []byte("key4"), v: []byte("val4"), op: PUT},
		{k: []byte("key4"), op: DELETE},
		{k: []byte("key3"), op: DELETE},
	}

	/* Init db1 and populate */
	db1, err := NewDB(TESTDBDIR)
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

	/* Use the same WAL to populate db2 and compare final values */
	db2, err := NewDB(TESTDBDIR)
	require.NoError(t, err)
	defer db2.Close()
	err = db2.Replay()
	require.NoError(t, err)

	tcs := []struct {
		k, v   []byte
		exists bool
	}{
		{k: []byte("key1"), v: []byte("val1"), exists: true},
		{k: []byte("key2"), v: []byte("val3"), exists: true},
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
	_, err := NewDB(TESTDBDIR)
	require.NoError(t, err)
	defer cleanupTestDB(t)
	for i := 1; i <= 5; i++ {
		/* Check if next sst filename for "test" directory correct - since dir is empty we should get "sst1", "sst2"...in order after creating each one */
		filenameWant := fmt.Sprintf("%s%d", DEFAULTSSTFILENAME, i)
		filenameGot, err := getNextSSTableName(TESTDBDIR)
		require.NoError(t, err)
		require.Equal(t, filenameWant, filenameGot)

		/* Create the sst filename */
		sstPath := filepath.Join(TESTDBDIR, filenameWant)
		_, err = os.Create(sstPath)
		require.NoError(t, err)
		defer os.Remove(sstPath)
	}
}
