package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type Iterator interface {
	// Next moves the iterator to the next key/value pair.
	// It returns false if the iterator is exhausted.
	Next() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key/value pair, or nil if done.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	Value() []byte
}

type IteratorTester struct {
	New func(DB) Iterator
}

func TestIteratorNext(t *testing.T, testerIter IteratorTester, testerDB DBTester) {
	db := testerDB.New()
	iterator := testerIter.New(db)
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