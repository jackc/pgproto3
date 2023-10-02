package pgproto3

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/jackc/pgio"
)

type Close struct {
	Object_Type byte   `json:"object_type" yaml:"object_type"`
	Name        string `json:"name" yaml:"name"`
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*Close) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *Close) Decode(src []byte) error {
	//println("Close.Decode")
	if len(src) < 2 {
		return &invalidMessageFormatErr{messageType: "Close"}
	}

	dst.Object_Type = src[0]
	rp := 1

	idx := bytes.IndexByte(src[rp:], 0)
	if idx != len(src[rp:])-1 {
		return &invalidMessageFormatErr{messageType: "Close"}
	}

	dst.Name = string(src[rp : len(src)-1])

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *Close) Encode(dst []byte) []byte {
	//println("Close.Encode")
	dst = append(dst, 'C')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)

	dst = append(dst, src.Object_Type)
	dst = append(dst, src.Name...)
	dst = append(dst, 0)

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src Close) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type        string
		Object_Type string
		Name        string
	}{
		Type:        "Close",
		Object_Type: string(src.Object_Type),
		Name:        src.Name,
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *Close) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}

	var msg struct {
		Object_Type string
		Name        string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	if len(msg.Object_Type) != 1 {
		return errors.New("invalid length for Close.Object_Type")
	}

	dst.Object_Type = byte(msg.Object_Type[0])
	dst.Name = msg.Name
	return nil
}
