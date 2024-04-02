package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

/* Test flags + errors for Open function */
func TestOpen(t *testing.T) {
	tcs := []struct {
		name             string
		flag             WALFlag
		expectedFileFlag int
		err              error
	}{
		{name: "error: both RDONLY and APPENDONLY chosen", err: ErrOnlyOnePrimaryModeAllowed, flag: 0x03},
		{name: "error: no file modeschosen", err: ErrOnlyOnePrimaryModeAllowed, flag: 0x00},
		{name: "error: invalid file mode", err: ErrOnlyOnePrimaryModeAllowed, flag: 0x04},
		/* Test that WALFlag sets the correct os.file flag */
		{name: "RDONLY flag set", flag: RDONLY, expectedFileFlag: os.O_RDONLY},
		{name: "WRONLY flag set", flag: WRONLY, expectedFileFlag: os.O_WRONLY | os.O_APPEND},
		{name: "RDONLY + CREATE flag set", flag: RDONLY | CREATE, expectedFileFlag: os.O_RDONLY | os.O_CREATE},
		{name: "WRONLY + TRUNC flag set", flag: WRONLY | TRUNC, expectedFileFlag: os.O_WRONLY | os.O_APPEND | os.O_TRUNC},
	}

	for _, tc := range tcs {
		log, err := Open("test", tc.flag)
		if err != nil {
			require.Error(t, err, tc.err)
		} else {
			require.Equal(t, tc.expectedFileFlag, log.fileFlag)
		}
	}
}
