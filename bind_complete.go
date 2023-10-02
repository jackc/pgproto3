package pgproto3

import (
	"encoding/json"
)

type BindComplete struct {
	// IsBindComplete string `json:"is_bind_complete" yaml:"is_bind_complete"`
}

// Backend identifies this message as sendable by the PostgreSQL backend.
func (*BindComplete) Backend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *BindComplete) Decode(src []byte) error {
	//println("BindComplete.Decode")
	if len(src) != 0 {
		return &invalidMessageLenErr{messageType: "BindComplete", expectedLen: 0, actualLen: len(src)}
	}
	// dst.IsBindComplete = "true"

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *BindComplete) Encode(dst []byte) []byte {
	//println("BindComplete.Encode")
	return append(dst, '2', 0, 0, 0, 4)

}

// MarshalJSON implements encoding/json.Marshaler.
func (src BindComplete) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "BindComplete",
	})
}
