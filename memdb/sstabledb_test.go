package memdb

import (
	"bytes"
	"io"
	"testing"

	"github.com/chettriyuvraj/leveldb-clone/common"
	"github.com/stretchr/testify/require"
)

/* For testing SSTable */
type BytesReadWriteSeekCloser struct {
	*bytes.Reader
}

func (b BytesReadWriteSeekCloser) Write(p []byte) (n int, err error) {
	curData, err := io.ReadAll(b)
	if err != nil {
		return -1, err
	}

	updatedData := append(curData, p...)
	b.Reader = bytes.NewReader(updatedData)
	return len(p), nil
}

func (b BytesReadWriteSeekCloser) Close() error {
	return nil
}

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
	reader := BytesReadWriteSeekCloser{bytes.NewReader(SSTableData)}
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
