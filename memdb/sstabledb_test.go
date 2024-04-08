package memdb

import (
	"bytes"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/stretchr/testify/require"
)

func TestGetSSTableDir(t *testing.T) {
	/* Test complete directory data */
	_, SSTableData, expectedDir := dummySSTableData()
	gotDir, err := getSSTableDir(SSTableData)
	require.NoError(t, err)
	require.Equal(t, expectedDir, gotDir)

	/* Test incomplete directory data - any incomplete data will be ignored */
	gotDir, err = getSSTableDir(SSTableData[:len(SSTableData)-3])
	expectedDir.entries = expectedDir.entries[:1]
	require.NoError(t, err)
	require.Equal(t, expectedDir, gotDir)
}

func TestSSTableGet(t *testing.T) {
	/* Mock SSTableDB to test */
	records, SSTableData, dir := dummySSTableData()
	reader := BytesReadSeekCloser{bytes.NewReader(SSTableData)}
	db := SSTableDB{f: reader, dir: dir}

	/* Check if each record found */
	for i := len(records) - 1; i >= 0; i-- {
		k, expectedV := records[i].k, records[i].v
		gotV, err := db.Get(k)
		require.NoError(t, err)
		require.Equal(t, expectedV, gotV)
	}

	/* Check for non-existent record */
	_, err := db.Get([]byte("randomVal"))
	require.ErrorIs(t, err, common.ErrKeyDoesNotExist)
}

/* For testing SSTableGet */
type BytesReadSeekCloser struct {
	*bytes.Reader
}

func (b BytesReadSeekCloser) Close() error {
	return nil
}
