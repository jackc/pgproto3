package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	src := pgproto3.CopyBothResponse{
		OverallFormat:     byte(1), // Just to differ from defaults
		ColumnFormatCodes: []uint16{0, 1},
	}
	dstBytes := []byte{}
	dstBytes = src.Encode(dstBytes)
	dst := pgproto3.CopyBothResponse{}
	err := dst.Decode(dstBytes[5:])
	assert.NoError(t, err, "No errors on decode")
	assert.Equal(t, dst.OverallFormat, src.OverallFormat, "OverallFormat is decoded successfully")
	assert.EqualValues(t, dst.ColumnFormatCodes, src.ColumnFormatCodes)
}
