package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	// "errors"

	"github.com/jackc/pgio"
)

type Parse struct {
	Name          string   `json:"name" yaml:"name"`
	Query         string   `json:"query" yaml:"query"`
	ParameterOIDs []uint32 `json:"parameter_oids" yaml:"parameter_oids"`
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*Parse) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
// use this method for decoding the message
func (dst *Parse) Decode(src []byte) error {
	// fmt.//println("Parse Decode")

	*dst = Parse{}

	buf := bytes.NewBuffer(src)

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.Name = string(b[:len(b)-1])
	//println("dst.Name -- ", dst.Name)
	b, err = buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.Query = string(b[:len(b)-1])
	//println("dst.Query -- ", dst.Query)
	if buf.Len() < 2 {
		return &invalidMessageFormatErr{messageType: "Parse"}
	}

	parameterOIDCount := int(binary.BigEndian.Uint16(buf.Next(2)))

	for i := 0; i < parameterOIDCount; i++ {
		if buf.Len() < 4 {
			return &invalidMessageFormatErr{messageType: "Parse"}
		}
		dst.ParameterOIDs = append(dst.ParameterOIDs, binary.BigEndian.Uint32(buf.Next(4)))
	}
	//println("parameterOIDCount -- ", dst.ParameterOIDs)
	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *Parse) Encode(dst []byte) []byte {
	//println("Parse.Encode")
	dst = append(dst, 'P')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)
	//println(src.Name, " -- ", src.Query)
	// src.Name = "stmtcache_1"
	// src.Query = "INSERT INTO products(name, price) VALUES($1, $2) RETURNING id"
	dst = append(dst, src.Name...)
	dst = append(dst, 0)
	dst = append(dst, src.Query...)
	dst = append(dst, 0)

	dst = pgio.AppendUint16(dst, uint16(len(src.ParameterOIDs)))
	for _, oid := range src.ParameterOIDs {
		dst = pgio.AppendUint32(dst, oid)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))
	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
// to store the data in json format you have to unmarshal it again
func (src Parse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type          string
		Name          string
		Query         string
		ParameterOIDs []uint32
	}{
		Type:          "Parse",
		Name:          src.Name,
		Query:         src.Query,
		ParameterOIDs: src.ParameterOIDs,
	})
}
