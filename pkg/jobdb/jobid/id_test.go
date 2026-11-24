package jobid

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestJobIDEncoding(test *testing.T) {
	for _, vector := range []struct{ hex, id string }{
		{"0000000000000000000000000000000000000000", "C2000000000000000000000000000"},
		{"000000000000000000000000000000000000003e", "C2000000000000000000000000010"},
		{"ffffffffffffffffffffffffffffffffffffffff", "C2aWgEPTl1tmebfsQzFP4bxwgy80V"},
	} {
		raw, err := hex.DecodeString(vector.hex)
		require.NoError(test, err)
		original, err := ksuid.FromBytes(raw)
		require.NoError(test, err)
		id := format(original)
		require.Equal(test, vector.id, id)
		require.Equal(test, original.String(), strings.TrimPrefix(id, "C2"))
	}
}

func TestJobIDNew(test *testing.T) {
	id := New()
	require.Regexp(test, `^C2[0-9A-Za-z]{27}$`, id)
	original, err := ksuid.Parse(strings.TrimPrefix(id, "C2"))
	require.NoError(test, err)
	require.Equal(test, id, format(original))
}
