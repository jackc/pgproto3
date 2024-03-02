package pgproto3

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode"

	"github.com/jackc/pgio"
)

type DataRow struct {
	Values    [][]byte `json:"values" yaml:"-"`
	RowValues []string `json:"row_values" yaml:"row_values,flow"`
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
			dst.Values[i] = nil //[]byte{255, 255, 255, 255}
		} else {
			if len(src[rp:]) < msgSize {
				return &invalidMessageFormatErr{messageType: "DataRow"}
			}

			dst.Values[i] = src[rp : rp+msgSize : rp+msgSize]
			rp += msgSize
		}
	}
	// fmt.Println("DECODED VALUES", dst.Values)
	dst.RowValues = []string{}
	for _, v := range dst.Values {
		// fmt.Println(string(v))
		bufStr := ""
		// if v == nil {
		// 	bufStr = "NIL"
		// 	dst.RowValues = append(dst.RowValues, bufStr)
		// 	continue
		// }
		if !IsAsciiPrintable(string(v)) {
			bufStr = "b64:" + base64.StdEncoding.EncodeToString(v)
		} else {
			bufStr = string(v)
		}
		dst.RowValues = append(dst.RowValues, bufStr)
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *DataRow) Encode(dst []byte) []byte {
	//println("DataRow.Encode")
	dst = append(dst, 'D')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)
	// src.Values = stringsToBytesArray(src.RowValues)

	// epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	// // Given date
	// givenDate := time.Date(2021, 7, 14, 0, 0, 0, 0, time.UTC)

	// Calculate the difference in days
	// difference := givenDate.Sub(epoch).Hours() / 24

	// Prepare a byte slice to hold the binary representation
	// buf := make([]byte, 4)
	// binary.BigEndian.PutUint32(buf, uint32(difference))

	// // Output the difference in days and the binary representation
	// fmt.Printf("Days difference: %d\n", int(difference))
	// fmt.Printf("Binary representation: %v\n", buf)
	if src.RowValues != nil && len(src.RowValues) > 0 {
		// fmt.Println("SRC ROW VALUES *** * ** * * ** ", src.RowValues)
		src.Values = stringsToBytesArray(src.RowValues)
	}
	// fmt.Println("SRC VALUES", src.Values)
	dst = pgio.AppendUint16(dst, uint16(len(src.Values)))
	for _, v := range src.Values {
		if v == nil || len(v) == 0{
			dst = pgio.AppendInt32(dst, -1)
			continue
		}
		
		
		dst = pgio.AppendInt32(dst, int32(len(v)))
		dst = append(dst, v...)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	// src.RowValues = []string{}
	// src.Values = [][]byte{}
	return dst
}

func stringsToBytesArray(strArray []string) [][]byte {
	byteArray := make([][]byte, len(strArray))

	for i, str := range strArray {
		if str == "NIL" {
			fmt.Println("NIL AHHAHAHAHAHHAHAHAHAH")
			byteArray[i] = []byte{255, 255, 255, 255}
			continue
		}
		byt, isValidBase64 := isValidBase64(str)
		if isValidBase64 && byt != nil {
			byteArray[i] = byt
		} else if IsAsciiPrintable(str) {
			byteArray[i] = []byte(str)
		}
	}

	return byteArray
}

func isValidBase64(s string) ([]byte, bool) {
	// check if it contains b64:
	// then slice the string and decode
	if len(s) < 5 {
		return nil, false
	}
	if s[:4] != "b64:" {
		return nil, false
	}
	s = s[4:]

	val, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	fmt.Println("VALUEEEEE", val, "HURRAY", s)
	return val, true
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
