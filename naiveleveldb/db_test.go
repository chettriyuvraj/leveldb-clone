package naiveleveldb

import (
	"fmt"
	"sort"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

func TestDB(t *testing.T) {
	test.TestDB(t, test.DBTester{New: NewLevelDBAsInterface})
	test.TestIterator(t, test.IteratorTester{New: NewLevelDBIteratorAsInterface}, test.DBTester{New: NewLevelDBAsInterface})
}

/* Workaround done exclusively to match signature with test suite */
func NewLevelDBAsInterface() common.DB {
	return &LevelDB{entries: []*DBEntry{}}
}

func NewLevelDBIteratorAsInterface(db common.DB) common.Iterator {
	return &LevelDBIterator{LevelDB: db.(*LevelDB), idx: 0}
}

/* This is an implementation-specific test, hence not exported to global test suite */
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

// func TestDelete(t *testing.T) {
// 	db := NewLevelDB()

// 	/* Delete non-existent key */
// 	keyNonExistent := []byte("kNE")
// 	err := db.Delete(keyNonExistent)
// 	require.ErrorIs(t, err, ErrKeyDoesNotExist)

// 	/* Delete existing key */
// 	k1, v1 := []byte("key1"), []byte("val1")
// 	err = db.Put(k1, v1)
// 	require.NoError(t, err)
// 	err = db.Delete(k1)
// 	require.NoError(t, err)
// 	_, err = db.Get(k1)
// 	require.ErrorIs(t, err, ErrKeyDoesNotExist)
// }

// func TestLevelDBIteratorNext(t *testing.T) {
// 	db := NewLevelDB()
// 	iterator := NewLevelDBIterator(db)
// 	iterations := 10

// 	/* Test Empty Iterator */
// 	iteratorTestNext(t, iterator, false, false)
// 	iteratorTestKey(t, iterator, []byte{}, false)
// 	iteratorTestVal(t, iterator, []byte{}, false)

// 	/* Populate db */
// 	for i := 0; i < iterations; i++ {
// 		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 		err := db.Put(k, v)
// 		require.NoError(t, err)
// 	}

// 	/* Move iterator over all values and check that they exist + are correct + no errors */
// 	for i := 0; i < iterations; i++ {
// 		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 		iteratorTestKey(t, iterator, keyExpected, false)
// 		iteratorTestVal(t, iterator, valExpected, false)
// 		if i < iterations-1 {
// 			iteratorTestNext(t, iterator, true, false)
// 		} else {
// 			iteratorTestNext(t, iterator, false, false)
// 		}
// 	}

// 	/* After all values are exhausted 1st two checks already tested in last iteration of loop, just keeping both checks to be consistent */
// 	iteratorTestNext(t, iterator, false, false)
// 	iteratorTestKey(t, iterator, []byte{}, false)
// 	iteratorTestVal(t, iterator, []byte{}, false)
// }

// func TestRangeScan(t *testing.T) {
// 	db := NewLevelDB()
// 	iterations := 9
// 	for i := iterations; i >= 0; i -= 2 {
// 		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 		err := db.Put(k, v)
// 		require.NoError(t, err)
// 	}

// 	/* Check exact ranges + confirm if values exhausted afterwards */
// 	start, end := []byte("key1"), []byte("key9")
// 	iterator, err := db.RangeScan(start, end)
// 	require.NoError(t, err)
// 	for i := 1; i <= 9; i += 2 {
// 		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 		iteratorTestKey(t, iterator, keyExpected, false)
// 		iteratorTestVal(t, iterator, valExpected, false)
// 		if i < iterations {
// 			iteratorTestNext(t, iterator, true, false)
// 		} else {
// 			iteratorTestNext(t, iterator, false, false)
// 		}
// 	}
// 	iteratorTestNext(t, iterator, false, false)
// 	iteratorTestKey(t, iterator, []byte{}, false)
// 	iteratorTestVal(t, iterator, []byte{}, false)

// 	/* Check inexact ranges + confirm if values exhausted afterwards */
// 	start, end = []byte("key"), []byte("key8")
// 	iterator, err = db.RangeScan(start, end)
// 	require.NoError(t, err)
// 	for i := 1; i <= 7; i += 2 {
// 		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 		iteratorTestKey(t, iterator, keyExpected, false)
// 		iteratorTestVal(t, iterator, valExpected, false)
// 		if i < 7 {
// 			iteratorTestNext(t, iterator, true, false)
// 		} else {
// 			iteratorTestNext(t, iterator, false, false)
// 		}
// 	}
// 	iteratorTestNext(t, iterator, false, false)
// 	iteratorTestKey(t, iterator, []byte{}, false)
// 	iteratorTestVal(t, iterator, []byte{}, false)
// }

// /* NOTE: This will potentially modify the iterator by calling Next() */
// func iteratorTestNext(t *testing.T, iterator Iterator, existsWant bool, errWant bool) {
// 	t.Helper()
// 	exists, err := iterator.Next(), iterator.Error()
// 	require.Equal(t, existsWant, exists)
// 	if errWant {
// 		require.Error(t, err)
// 	} else {
// 		require.NoError(t, err)
// 	}
// }

// func iteratorTestKey(t *testing.T, iterator Iterator, keyWant []byte, errWant bool) {
// 	t.Helper()
// 	keyGot, err := iterator.Key(), iterator.Error()
// 	require.Equal(t, keyWant, keyGot)
// 	if errWant {
// 		require.Error(t, err)
// 	} else {
// 		require.NoError(t, err)
// 	}
// }

// func iteratorTestVal(t *testing.T, iterator Iterator, valWant []byte, errWant bool) {
// 	t.Helper()
// 	t.Helper()
// 	valGot, err := iterator.Value(), iterator.Error()
// 	require.Equal(t, valWant, valGot)
// 	if errWant {
// 		require.Error(t, err)
// 	} else {
// 		require.NoError(t, err)
// 	}
// }

// func BenchmarkPut(b *testing.B) {
// 	bms := []struct {
// 		name string
// 		size int
// 	}{
// 		{name: "Hundred", size: 100},
// 		{name: "Thousand", size: 1000},
// 		{name: "TenThousand", size: 10000},
// 		// {name: "HundredThousand", size: 100000},
// 		// {name: "Million", size: 100000},
// 	}
// 	for _, bm := range bms {
// 		b.Run(bm.name, func(b *testing.B) {
// 			db := NewLevelDB()
// 			for i := 0; i < bm.size; i++ {
// 				k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 				db.Put(k, v)
// 			}
// 		})
// 	}
// }

// func benchmarkGet(b *testing.B, dbSize int) {
// 	/* Populate KV store */
// 	db := NewLevelDB()
// 	for i := 0; i < dbSize; i++ {
// 		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 		err := db.Put(k, v)
// 		require.NoError(b, err)
// 	}

// 	r := rand.New(rand.NewSource(500))

// 	bms := []struct {
// 		name string
// 		size int
// 	}{
// 		{name: "Hundred", size: 100},
// 		{name: "Thousand", size: 1000},
// 		{name: "TenThousand", size: 10000},
// 		// {name: "HundredThousand", size: 100000},
// 		// {name: "Million", size: 100000},
// 	}
// 	for _, bm := range bms {
// 		b.Run(bm.name, func(b *testing.B) {
// 			for i := 0; i < bm.size; i++ {
// 				k := []byte(fmt.Sprintf("key%d", r.Intn(dbSize)))
// 				_, err := db.Get(k)
// 				require.NoError(b, err)
// 			}
// 		})
// 	}
// }

// func BenchmarkGetFromThousand(b *testing.B) {
// 	benchmarkGet(b, 1000)
// }

// func BenchmarkGetFromTenThousand(b *testing.B) {
// 	benchmarkGet(b, 10000)
// }

// func BenchmarkGetFromHundredThousand(b *testing.B) {
// 	benchmarkGet(b, 100000)
// }

// func benchmarkDelete(b *testing.B, dbSize int) {

// 	bms := []struct {
// 		name string
// 		size int
// 	}{
// 		{name: "Hundred", size: 100},
// 		{name: "Thousand", size: 1000},
// 		{name: "TenThousand", size: 10000},
// 		// {name: "HundredThousand", size: 100000},
// 		// {name: "Million", size: 100000},
// 	}
// 	for _, bm := range bms {

// 		if bm.size > dbSize {
// 			b.Skip()
// 		}

// 		b.Run(bm.name, func(b *testing.B) {

// 			/* Each bm is run multiple times, so db needs to be repopulated for deletions each time - using ResetTimer to ignore setup cost */
// 			db := NewLevelDB()
// 			for i := 0; i < dbSize; i++ {
// 				k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
// 				err := db.Put(k, v)
// 				require.NoError(b, err)
// 			}

// 			/* Deletions are done in order, results for random deletes would be different */
// 			b.ResetTimer()
// 			for i := 0; i < bm.size; i++ {
// 				k := []byte(fmt.Sprintf("key%d", i))
// 				err := db.Delete(k)
// 				require.NoError(b, err)
// 			}
// 		})
// 	}
// }

// func BenchmarkDeleteFromThousand(b *testing.B) {
// 	benchmarkDelete(b, 1000)
// }

// func BenchmarkDeleteFromTenThousand(b *testing.B) {
// 	benchmarkDelete(b, 10000)
// }

// func BenchmarkDeleteFromHundredThousand(b *testing.B) {
// 	benchmarkDelete(b, 100000)
// }
