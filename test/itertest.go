package test

import (
	"fmt"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/stretchr/testify/require"
)

type IteratorTester struct {
	New func(common.DB) common.Iterator
}

func testIteratorNext(t *testing.T, testerIter IteratorTester, testerDB DBTester) {
	db := testerDB.New()
	iterator := testerIter.New(db)
	iterations := 10

	/* Test Empty Iterator */
	IteratorTestNext(t, iterator, false, false)
	IteratorTestKey(t, iterator, nil, false)
	IteratorTestVal(t, iterator, nil, false)

	/* Populate db */
	for i := 0; i < iterations; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Move iterator over all values and check that they exist + are correct + no errors */
	for i := 0; i < iterations; i++ {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		IteratorTestKey(t, iterator, keyExpected, false)
		IteratorTestVal(t, iterator, valExpected, false)
		if i < iterations-1 {
			IteratorTestNext(t, iterator, true, false)
		} else {
			IteratorTestNext(t, iterator, false, false)
		}
	}

	/* After all values are exhausted 1st two checks already tested in last iteration of loop, just keeping both checks to be consistent */
	IteratorTestNext(t, iterator, false, false)
	IteratorTestKey(t, iterator, nil, false)
	IteratorTestVal(t, iterator, nil, false)
}

/* NOTE: This will potentially modify the iterator by calling Next() */
func IteratorTestNext(t *testing.T, iterator common.Iterator, existsWant bool, errWant bool) {
	t.Helper()
	exists, err := iterator.Next(), iterator.Error()
	require.Equal(t, existsWant, exists)
	if errWant {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
}

func IteratorTestKey(t *testing.T, iterator common.Iterator, keyWant []byte, errWant bool) {
	t.Helper()
	keyGot, err := iterator.Key(), iterator.Error()
	require.Equal(t, keyWant, keyGot)
	if errWant {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
}

func IteratorTestVal(t *testing.T, iterator common.Iterator, valWant []byte, errWant bool) {
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
