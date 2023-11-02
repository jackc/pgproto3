package pgproto3

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"unicode"

	"github.com/jackc/pgio"
)

type DataRow struct {
	Values    [][]byte `json:"values" yaml:"-"`
	RowValues []string `json:"row_values" yaml:"row_values"`
}

// Backend identifies this message as sendable by the PostgreSQL backend.
func (*DataRow) Backend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *DataRow) Decode(src []byte) error {
	//println("DataRow.Decode")
	if len(src) < 2 {
		return &invalidMessageFormatErr{messageType: "DataRow"}
	}
	rp := 0
	fieldCount := int(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	// If the capacity of the values slice is too small OR substantially too
	// large reallocate. This is too avoid one row with many columns from
	// permanently allocating memory.
	if cap(dst.Values) < fieldCount || cap(dst.Values)-fieldCount > 32 {
		newCap := 32
		if newCap < fieldCount {
			newCap = fieldCount
		}
		dst.Values = make([][]byte, fieldCount, newCap)
	} else {
		dst.Values = dst.Values[:fieldCount]
	}

	for i := 0; i < fieldCount; i++ {
		if len(src[rp:]) < 4 {
			return &invalidMessageFormatErr{messageType: "DataRow"}
		}

		msgSize := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		// null
		if msgSize == -1 {
			dst.Values[i] = nil
		} else {
			if len(src[rp:]) < msgSize {
				return &invalidMessageFormatErr{messageType: "DataRow"}
			}

			dst.Values[i] = src[rp : rp+msgSize : rp+msgSize]
			rp += msgSize
		}
	}
	for _, v := range dst.Values {
		// fmt.Println(string(v))
		bufStr := ""
		if !IsAsciiPrintable(string(v)) {
			bufStr = base64.StdEncoding.EncodeToString(v)
			bufStr = "base64:" + bufStr
			dst.RowValues = append(dst.RowValues, bufStr)
			// println("NON PRINTABLE STRING FOUND !")
			continue
		}
		dst.RowValues = append(dst.RowValues, string(v))
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *DataRow) Encode(dst []byte) []byte {
	//println("DataRow.Encode")
	dst = append(dst, 'D')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)
	src.Values = stringsToBytesArray(src.RowValues)
	dst = pgio.AppendUint16(dst, uint16(len(src.Values)))
	for _, v := range src.Values {
		if v == nil {
			dst = pgio.AppendInt32(dst, -1)
			continue
		}

		dst = pgio.AppendInt32(dst, int32(len(v)))
		dst = append(dst, v...)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

func stringsToBytesArray(strArray []string) [][]byte {
	byteArray := make([][]byte, len(strArray))

	for i, str := range strArray {
		// if str[0] == 'b' && str[1] == 'a' && str[2] == 's' && str[3] == 'e' {
		// 	// slic and decode
		// 	byteArray[i], _ = base64.StdEncoding.DecodeString(str[7:])
		// 	continue
		// }
		byteArray[i] = []byte(str)
	}

	return byteArray
}

// checks if s is ascii and printable, aka doesn't include tab, backspace, etc.
func IsAsciiPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || (!unicode.IsPrint(r) && r != '\r' && r != '\n') {
			return false
		}
	}
	return true
}

// MarshalJSON implements encoding/json.Marshaler.
func (src DataRow) MarshalJSON() ([]byte, error) {
	formattedValues := make([]map[string]string, len(src.Values))
	for i, v := range src.Values {
		if v == nil {
			continue
		}

		var hasNonPrintable bool
		for _, b := range v {
			if b < 32 {
				hasNonPrintable = true
				break
			}
		}

		if hasNonPrintable {
			formattedValues[i] = map[string]string{"binary": hex.EncodeToString(v)}
		} else {
			formattedValues[i] = map[string]string{"text": string(v)}
		}
	}

	return json.Marshal(struct {
		Type   string
		Values []map[string]string
	}{
		Type:   "DataRow",
		Values: formattedValues,
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *DataRow) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}

	var msg struct {
		Values []map[string]string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	dst.Values = make([][]byte, len(msg.Values))
	for n, parameter := range msg.Values {
		var err error
		dst.Values[n], err = getValueFromJSON(parameter)
		if err != nil {
			return err
		}
	}
	return nil
}
