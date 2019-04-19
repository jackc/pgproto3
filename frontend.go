package pgproto3

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Frontend struct {
	cr ChunkReader
	w  io.Writer

	fieldDescriptionBlock []FieldDescription
	rowDescriptionBlock   []RowDescription

	byteSliceBlock [][]byte
	dataRowBlock   []DataRow

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

	msgBody, err := b.cr.Next(b.bodyLen)
	if err != nil {
		return nil, err
	}

	b.partialMsg = false

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
		if len(msgBody) < 2 {
			return nil, &invalidMessageFormatErr{messageType: "DataRow"}
		}
		fieldCount := int(binary.BigEndian.Uint16(msgBody))

		if len(b.byteSliceBlock) < fieldCount {
			b.byteSliceBlock = make([][]byte, fieldCount*128)
		}

		values := b.byteSliceBlock[:fieldCount]
		b.byteSliceBlock = b.byteSliceBlock[fieldCount:]

		if len(b.dataRowBlock) == 0 {
			b.dataRowBlock = make([]DataRow, 128)
		}

		dataRow := &b.dataRowBlock[0]
		b.dataRowBlock = b.dataRowBlock[1:]

		dataRow.Values = values

		msg = dataRow
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
		if len(msgBody) < 2 {
			return nil, &invalidMessageFormatErr{messageType: "RowDescription"}
		}
		fieldCount := int(binary.BigEndian.Uint16(msgBody))

		if len(b.fieldDescriptionBlock) < fieldCount {
			b.fieldDescriptionBlock = make([]FieldDescription, fieldCount*32)
		}

		fields := b.fieldDescriptionBlock[:fieldCount]
		b.fieldDescriptionBlock = b.fieldDescriptionBlock[fieldCount:]

		if len(b.rowDescriptionBlock) == 0 {
			b.rowDescriptionBlock = make([]RowDescription, 32)
		}

		rowDescription := &b.rowDescriptionBlock[0]
		b.rowDescriptionBlock = b.rowDescriptionBlock[1:]

		rowDescription.Fields = fields
		msg = rowDescription
	case 'V':
		msg = &FunctionCallResponse{}
	case 'W':
		msg = &CopyBothResponse{}
	case 'Z':
		msg = &ReadyForQuery{}
	default:
		return nil, errors.Errorf("unknown message type: %c", b.msgType)
	}

	err = msg.Decode(msgBody)
	return msg, err
}
