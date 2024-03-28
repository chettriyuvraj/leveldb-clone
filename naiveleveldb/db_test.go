package naiveleveldb

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPut(t *testing.T) {
	db := NewLevelDB()

	/* Get-Put a non existing key */
	keyNonExistent := []byte("kNE")
	_, err := db.Get(keyNonExistent)
	require.ErrorIs(t, err, ErrKeyDoesNotExist)

	/* Get-Put a new key-value pair */
	k1, v1 := []byte("key1"), []byte("val1")
	err = db.Put(k1, v1)
	require.NoError(t, err)
	v1FromDB, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v1, v1FromDB)

	/* Overwrite an existing val */
	v2 := []byte("val2")
	err = db.Put(k1, v2)
	require.NoError(t, err)
	v2FromDB, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v2, v2FromDB)
}

func TestPutSorted(t *testing.T) {
	db := NewLevelDB()
	iterations := 10
	for i := iterations; i >= 0; i-- {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
		require.True(t, sort.IsSorted(DBEntrySlice(db.entries)))
	}
}

func TestDelete(t *testing.T) {
	db := NewLevelDB()

	/* Delete non-existent key */
	keyNonExistent := []byte("kNE")
	err := db.Delete(keyNonExistent)
	require.ErrorIs(t, err, ErrKeyDoesNotExist)

	/* Delete existing key */
	k1, v1 := []byte("key1"), []byte("val1")
	err = db.Put(k1, v1)
	require.NoError(t, err)
	err = db.Delete(k1)
	require.NoError(t, err)
	_, err = db.Get(k1)
	require.ErrorIs(t, err, ErrKeyDoesNotExist)
}

func TestLevelDBIteratorNext(t *testing.T) {
	db := NewLevelDB()
	iterator := NewLevelDBIterator(db)
	iterations := 10

	/* Test Empty Iterator */
	iteratorTestNext(t, iterator, false, false)
	iteratorTestKey(t, iterator, []byte{}, false)
	iteratorTestVal(t, iterator, []byte{}, false)

	/* Populate db */
	for i := 0; i < iterations; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Move iterator over all values and check that they exist + are correct + no errors */
	for i := 0; i < iterations; i++ {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		iteratorTestKey(t, iterator, keyExpected, false)
		iteratorTestVal(t, iterator, valExpected, false)
		if i < iterations-1 {
			iteratorTestNext(t, iterator, true, false)
		} else {
			iteratorTestNext(t, iterator, false, false)
		}
	}

	/* After all values are exhausted 1st two checks already tested in last iteration of loop, just keeping both checks to be consistent */
	iteratorTestNext(t, iterator, false, false)
	iteratorTestKey(t, iterator, []byte{}, false)
	iteratorTestVal(t, iterator, []byte{}, false)
}

func TestRangeScan(t *testing.T) {
	db := NewLevelDB()
	iterations := 9
	for i := iterations; i >= 0; i -= 2 {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Check exact ranges + confirm if values exhausted afterwards */
	start, end := []byte("key1"), []byte("key9")
	iterator, err := db.RangeScan(start, end)
	require.NoError(t, err)
	for i := 1; i <= 9; i += 2 {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		iteratorTestKey(t, iterator, keyExpected, false)
		iteratorTestVal(t, iterator, valExpected, false)
		if i < iterations {
			iteratorTestNext(t, iterator, true, false)
		} else {
			iteratorTestNext(t, iterator, false, false)
		}
	}
	iteratorTestNext(t, iterator, false, false)
	iteratorTestKey(t, iterator, []byte{}, false)
	iteratorTestVal(t, iterator, []byte{}, false)

	/* Check inexact ranges + confirm if values exhausted afterwards */
	start, end = []byte("key"), []byte("key8")
	iterator, err = db.RangeScan(start, end)
	require.NoError(t, err)
	for i := 1; i <= 7; i += 2 {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		iteratorTestKey(t, iterator, keyExpected, false)
		iteratorTestVal(t, iterator, valExpected, false)
		if i < 7 {
			iteratorTestNext(t, iterator, true, false)
		} else {
			iteratorTestNext(t, iterator, false, false)
		}
	}
	iteratorTestNext(t, iterator, false, false)
	iteratorTestKey(t, iterator, []byte{}, false)
	iteratorTestVal(t, iterator, []byte{}, false)
}

/* NOTE: This will potentially modify the iterator by calling Next() */
func iteratorTestNext(t *testing.T, iterator Iterator, existsWant bool, errWant bool) {
	t.Helper()
	exists, err := iterator.Next(), iterator.Error()
	require.Equal(t, existsWant, exists)
	if errWant {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
}

func iteratorTestKey(t *testing.T, iterator Iterator, keyWant []byte, errWant bool) {
	t.Helper()
	keyGot, err := iterator.Key(), iterator.Error()
	require.Equal(t, keyWant, keyGot)
	if errWant {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
}

func iteratorTestVal(t *testing.T, iterator Iterator, valWant []byte, errWant bool) {
	t.Helper()
	t.Helper()
	valGot, err := iterator.Value(), iterator.Error()
	require.Equal(t, valWant, valGot)
	if errWant {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
}
