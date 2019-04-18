package pgproto3

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Frontend struct {
	cr ChunkReader
	w  io.Writer

	bodyLen    int
	msgType    byte
	partialMsg bool
}

func NewFrontend(cr ChunkReader, w io.Writer) (*Frontend, error) {
	return &Frontend{cr: cr, w: w}, nil
}

func (b *Frontend) Send(msg FrontendMessage) error {
	_, err := b.w.Write(msg.Encode(nil))
	return err
}

func (b *Frontend) Receive() (BackendMessage, error) {
	if !b.partialMsg {
		header, err := b.cr.Next(5)
		if err != nil {
			return nil, err
		}

		b.msgType = header[0]
		b.bodyLen = int(binary.BigEndian.Uint32(header[1:])) - 4
		b.partialMsg = true
	}

	var msg BackendMessage
	switch b.msgType {
	case '1':
		msg = &ParseComplete{}
	case '2':
		msg = &BindComplete{}
	case '3':
		msg = &CloseComplete{}
	case 'A':
		msg = &NotificationResponse{}
	case 'c':
		msg = &CopyDone{}
	case 'C':
		msg = &CommandComplete{}
	case 'd':
		msg = &CopyData{}
	case 'D':
		msg = &DataRow{}
	case 'E':
		msg = &ErrorResponse{}
	case 'f':
		msg = &CopyFail{}
	case 'G':
		msg = &CopyInResponse{}
	case 'H':
		msg = &CopyOutResponse{}
	case 'I':
		msg = &EmptyQueryResponse{}
	case 'K':
		msg = &BackendKeyData{}
	case 'n':
		msg = &NoData{}
	case 'N':
		msg = &NoticeResponse{}
	case 'R':
		msg = &Authentication{}
	case 'S':
		msg = &ParameterStatus{}
	case 't':
		msg = &ParameterDescription{}
	case 'T':
		msg = &RowDescription{}
	case 'V':
		msg = &FunctionCallResponse{}
	case 'W':
		msg = &CopyBothResponse{}
	case 'Z':
		msg = &ReadyForQuery{}
	default:
		return nil, errors.Errorf("unknown message type: %c", b.msgType)
	}

	msgBody, err := b.cr.Next(b.bodyLen)
	if err != nil {
		return nil, err
	}

	b.partialMsg = false

	err = msg.Decode(msgBody)
	return msg, err
}
