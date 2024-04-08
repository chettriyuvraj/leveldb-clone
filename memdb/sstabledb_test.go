package memdb

import (
	"testing"

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
