package db

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
	exists, err := iterator.Next(), iterator.Error()
	require.NoError(t, err)
	require.False(t, exists)
	keyGot, err := iterator.Key(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, keyGot)
	valGot, err := iterator.Value(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, valGot)

	/* Populate db */
	for i := 0; i < iterations; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Move iterator over all values and check that they exist + are correct + no errors */
	for i := 0; i < iterations; i++ {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		keyGot, err := iterator.Key(), iterator.Error()
		require.NoError(t, err)
		require.Equal(t, keyExpected, keyGot)
		valGot, err := iterator.Value(), iterator.Error()
		require.NoError(t, err)
		require.Equal(t, valExpected, valGot)

		nextExists, err := iterator.Next(), iterator.Error()
		require.NoError(t, err)
		if i < iterations-1 {
			require.True(t, nextExists)
		} else {
			require.False(t, nextExists)
		}
	}

	/* After all values are exhausted 1st two checks already tested in last iteration of loop, just keeping both checks to be consistent */
	exists, err = iterator.Next(), iterator.Error()
	require.NoError(t, err)
	require.False(t, exists)
	keyGot, err = iterator.Key(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, keyGot)
	valGot, err = iterator.Value(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, valGot)
}

func TestRangeScan(t *testing.T) {
	db := NewLevelDB()
	iterations := 9
	for i := iterations; i >= 0; i -= 2 {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Check exact ranges - also confirm once values exhausted */
	start, end := []byte("key1"), []byte("key9")
	iterator, err := db.RangeScan(start, end)
	require.NoError(t, err)
	for i := 1; i <= 9; i += 2 {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		keyGot, err := iterator.Key(), iterator.Error()
		require.NoError(t, err)
		require.Equal(t, keyExpected, keyGot)
		valGot, err := iterator.Value(), iterator.Error()
		require.NoError(t, err)
		require.Equal(t, valExpected, valGot)

		nextExists, err := iterator.Next(), iterator.Error()
		require.NoError(t, err)
		if i < 9 {
			require.True(t, nextExists)
		} else {
			require.False(t, nextExists)
		}
	}

	exists, err := iterator.Next(), iterator.Error()
	require.NoError(t, err)
	require.False(t, exists)
	keyGot, err := iterator.Key(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, keyGot)
	valGot, err := iterator.Value(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, valGot)

	/* Check inexact ranges - also confirm once values exhausted */
	start, end = []byte("key"), []byte("key8")
	iterator, err = db.RangeScan(start, end)
	require.NoError(t, err)
	for i := 1; i <= 7; i += 2 {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		keyGot, err := iterator.Key(), iterator.Error()
		require.NoError(t, err)
		require.Equal(t, keyExpected, keyGot)
		valGot, err := iterator.Value(), iterator.Error()
		require.NoError(t, err)
		require.Equal(t, valExpected, valGot)

		nextExists, err := iterator.Next(), iterator.Error()
		require.NoError(t, err)
		if i < 7 {
			require.True(t, nextExists)
		} else {
			require.False(t, nextExists)
		}
	}

	exists, err = iterator.Next(), iterator.Error()
	require.NoError(t, err)
	require.False(t, exists)
	keyGot, err = iterator.Key(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, keyGot)
	valGot, err = iterator.Value(), iterator.Error()
	require.NoError(t, err)
	require.Equal(t, []byte{}, valGot)
}
