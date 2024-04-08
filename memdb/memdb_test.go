package memdb

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/chettriyuvraj/leveldb-clone/skiplist"
	"github.com/chettriyuvraj/leveldb-clone/test"
	"github.com/stretchr/testify/require"
)

/* Workaround done exclusively to match signature with test suite */
func newMemDBAsInterface() common.DB {
	return &MemDB{*skiplist.NewSkipList(P, MAXLEVEL)}
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

/* Implementation-specific tests */

func TestFullScan(t *testing.T) {
	db, err := NewMemDB()
	require.NoError(t, err)

	/* Test empty memdb */
	iter, err := db.FullScan()
	require.NoError(t, err)
	test.IteratorTestNext(t, iter, false, false)
	test.IteratorTestKey(t, iter, nil, false)
	test.IteratorTestVal(t, iter, nil, false)

	/* Populate db */
	for i := 1; i <= 9; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := db.Put(k, v)
		require.NoError(t, err)
	}

	/* Check if all values obtained using FullScan + no error and nil when exhausted */
	iter, err = db.FullScan()
	require.NoError(t, err)
	for i := 1; i <= 9; i++ {
		keyExpected, valExpected := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		test.IteratorTestKey(t, iter, keyExpected, false)
		test.IteratorTestVal(t, iter, valExpected, false)
		if i < 9 {
			test.IteratorTestNext(t, iter, true, false)
		} else {
			test.IteratorTestNext(t, iter, false, false)
			test.IteratorTestKey(t, iter, nil, false)
			test.IteratorTestVal(t, iter, nil, false)
		}
	}

}

func TestGetSSTableData(t *testing.T) {
	db, err := NewMemDB()
	require.NoError(t, err)

	/* Empty SSTable err check */
	_, err = db.getSSTableData()
	require.Error(t, err, ErrNoSSTableDataToWrite)

	/* Populate db + compute expected result by hand in duumySSTableData func */
	records, expectedSSTableData, _ := dummySSTableData()
	for _, record := range records {
		err = db.Put(record.k, record.v)
		require.NoError(t, err)
	}

	got, err := db.getSSTableData()
	require.NoError(t, err)
	require.Equal(t, expectedSSTableData, got)

}

/*
- Hand-computed data for testing
*/
func dummySSTableData() (records []struct{ k, v []byte }, encodedSSTableData []byte, dir SSTableDirectory) {
	k1, v1 := []byte("comp"), []byte("computers")
	k2, v2 := []byte("extc"), []byte{}

	/* Compute key:val data by hand */
	k1len := []byte{0x00, 0x00, 0x00, 0x04}
	k1pluslen := append(k1len, k1...)
	v1len := []byte{0x00, 0x00, 0x00, 0x09}
	v1pluslen := append(v1len, v1...)
	k2len := []byte{0x00, 0x00, 0x00, 0x04}
	k2pluslen := append(k2len, k2...)
	v2len := []byte{0x00, 0x00, 0x00, 0x00}
	v2pluslen := append(v2len, v2...)
	kvData1 := append(k1pluslen, v1pluslen...)
	kvData2 := append(k2pluslen, v2pluslen...)
	kvData := append(kvData1, kvData2...)
	dirOffset := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29} /* 41 -> len(kvData) + 8 bytes for dirOffset at the start */
	/* Compute directory data */
	k1Offset := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}
	k2Offset := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1D} /* (8 + 21) since the first 8 bytes include the dirOffset */
	d1 := append(k1len, append(k1, k1Offset...)...)
	d2 := append(k2len, append(k2, k2Offset...)...)
	encodedDir := append(d1, d2...)
	/* Combine */
	e1 := append(dirOffset, kvData...)
	expected := append(e1, encodedDir...)

	dir = SSTableDirectory{
		entries: []SSTableDirEntry{
			{k1, binary.BigEndian.Uint64(k1Offset[:])},
			{k2, binary.BigEndian.Uint64(k2Offset[:])},
		},
	}

	return []struct{ k, v []byte }{{k1, v1}, {k2, v2}}, expected, dir
}
