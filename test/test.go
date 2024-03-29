package test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/stretchr/testify/require"
)

/* TODO: Range Scan benchmarks */

type DBTester struct {
	New func() common.DB
}

func TestDB(t *testing.T, tester DBTester) {
	tcs := []struct {
		name string
		f    func(t *testing.T, tester DBTester)
	}{
		{"testGetPut", testGetPut},
		{"testDelete", testDelete},
		{"testRangeScan", testRangeScan},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.f(t, tester)
		})
	}
}

func TestIterator(t *testing.T, testerIter IteratorTester, testerDB DBTester) {
	tcs := []struct {
		name string
		f    func(t *testing.T, tester IteratorTester, testerDB DBTester)
	}{
		{"TestIteratorNext", testIteratorNext},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.f(t, testerIter, testerDB)
		})
	}
}

func BenchmarkDB(b *testing.B, tester DBTester) {
	bms := []struct {
		name string
		f    func(b *testing.B, tester DBTester)
	}{
		{name: "benchmarkPut", f: benchmarkPut},
		{name: "benchmarkGet", f: benchmarkGet},
		{name: "benchmarkDelete", f: benchmarkDelete},
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			bm.f(b, tester)
		})
	}
}

func testGetPut(t *testing.T, tester DBTester) {
	db := tester.New()

	/* Get-Put a non existing key */
	keyNonExistent := []byte("kNE")
	_, err := db.Get(keyNonExistent)
	require.ErrorIs(t, err, common.ErrKeyDoesNotExist)

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

func testDelete(t *testing.T, tester DBTester) {
	db := tester.New()

	/* Delete non-existent key */
	keyNonExistent := []byte("kNE")
	err := db.Delete(keyNonExistent)
	require.ErrorIs(t, err, common.ErrKeyDoesNotExist)

	/* Delete existing key */
	k1, v1 := []byte("key1"), []byte("val1")
	err = db.Put(k1, v1)
	require.NoError(t, err)
	err = db.Delete(k1)
	require.NoError(t, err)
	_, err = db.Get(k1)
	require.ErrorIs(t, err, common.ErrKeyDoesNotExist)
}

func testRangeScan(t *testing.T, tester DBTester) {
	db := tester.New()
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

func benchmarkPut(b *testing.B, tester DBTester) {
	bms := []struct {
		name string
		size int
	}{
		{name: "Hundred", size: 100},
		{name: "Thousand", size: 1000},
		{name: "TenThousand", size: 10000},
		{name: "HundredThousand", size: 100000},
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			db := tester.New()
			for i := 0; i < bm.size; i++ {
				k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
				db.Put(k, v)
			}
		})
	}
}

func benchmarkGet(b *testing.B, tester DBTester) {
	benchmarkGetFromThousand := func(b *testing.B, tester DBTester) {
		benchmarkGetHelper(b, tester, 1000)
	}
	benchmarkGetFromTenThousand := func(b *testing.B, tester DBTester) {
		benchmarkGetHelper(b, tester, 10000)
	}

	bms := []struct {
		name string
		f    func(b *testing.B, tester DBTester)
	}{
		{name: "DBSize:Thousand", f: benchmarkGetFromThousand},
		{name: "DBSize:Ten-Thousand", f: benchmarkGetFromTenThousand},
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			bm.f(b, tester)
		})
	}
}

func benchmarkGetHelper(b *testing.B, tester DBTester, dbSize int) {
	/* Populate KV store */
	db := tester.New()
	for i := 0; i < dbSize; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(b, err)
	}

	r := rand.New(rand.NewSource(500))

	bms := []struct {
		name string
		size int
	}{
		{name: "Hundred", size: 100},
		{name: "Thousand", size: 1000},
		{name: "TenThousand", size: 10000},
		{name: "HundredThousand", size: 100000},
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < bm.size; i++ {
				k := []byte(fmt.Sprintf("key%d", r.Intn(dbSize)))
				_, err := db.Get(k)
				require.NoError(b, err)
			}
		})
	}
}

func benchmarkDelete(b *testing.B, tester DBTester) {
	benchmarkDeleteFromThousand := func(b *testing.B, tester DBTester) {
		benchmarkDeleteHelper(b, tester, 1000)
	}
	benchmarkDeleteFromTenThousand := func(b *testing.B, tester DBTester) {
		benchmarkDeleteHelper(b, tester, 10000)
	}

	bms := []struct {
		name string
		f    func(b *testing.B, tester DBTester)
	}{
		{name: "DBSize:Thousand", f: benchmarkDeleteFromThousand},
		{name: "DBSize:Ten-Thousand", f: benchmarkDeleteFromTenThousand},
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			bm.f(b, tester)
		})
	}
}

func benchmarkDeleteHelper(b *testing.B, tester DBTester, dbSize int) {

	bms := []struct {
		name string
		size int
	}{
		{name: "Hundred", size: 100},
		{name: "Thousand", size: 1000},
		{name: "TenThousand", size: 10000},
		{name: "HundredThousand", size: 100000},
	}
	for _, bm := range bms {

		if bm.size > dbSize {
			b.Skip()
		}

		b.Run(bm.name, func(b *testing.B) {

			/* Each bm is run multiple times, so db needs to be repopulated for deletions each time - using ResetTimer to ignore setup cost */
			db := tester.New()
			for i := 0; i < dbSize; i++ {
				k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
				err := db.Put(k, v)
				require.NoError(b, err)
			}

			/* Deletions are done in order, results for random deletes would be different */
			b.ResetTimer()
			for i := 0; i < bm.size; i++ {
				k := []byte(fmt.Sprintf("key%d", i))
				err := db.Delete(k)
				require.NoError(b, err)
			}
		})
	}
}
