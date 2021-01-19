package databank

import "github.com/edge/databank/pkg/convert"

// ReadInt16 from entry content.
func (e *Entry) ReadInt16() int16 {
	b := e.Content
	return convert.BytesToInt16([2]byte{b[0], b[1]})
}

// ReadInt32 from entry content.
func (e *Entry) ReadInt32() int32 {
	b := e.Content
	return convert.BytesToInt32([4]byte{b[0], b[1], b[2], b[3]})
}

// ReadInt64 from entry content.
func (e *Entry) ReadInt64() int64 {
	b := e.Content
	return convert.BytesToInt64([8]byte{b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]})
}

// ReadString from entry content.
func (e *Entry) ReadString() string {
	return convert.BytesToString(e.Content)
}

// ReadUint16 from entry content.
func (e *Entry) ReadUint16() uint16 {
	b := e.Content
	return convert.BytesToUint16([2]byte{b[0], b[1]})
}

// ReadUint32 from entry content.
func (e *Entry) ReadUint32() uint32 {
	b := e.Content
	return convert.BytesToUint32([4]byte{b[0], b[1], b[2], b[3]})
}

// ReadUint64 from entry content.
func (e *Entry) ReadUint64() uint64 {
	b := e.Content
	return convert.BytesToUint64([8]byte{b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]})
}

// WriteInt16 to entry content.
func (e *Entry) WriteInt16(v int16) {
	b := convert.Int16ToBytes(v)
	e.Content = b[0:2]
}

// WriteInt32 to entry content.
func (e *Entry) WriteInt32(v int32) {
	b := convert.Int32ToBytes(v)
	e.Content = b[0:4]
}

// WriteInt64 to entry content.
func (e *Entry) WriteInt64(v int64) {
	b := convert.Int64ToBytes(v)
	e.Content = b[0:8]
}

// WriteString to entry content.
func (e *Entry) WriteString(v string) {
	e.Content = convert.StringToBytes(v)
}

// WriteUint16 to entry content.
func (e *Entry) WriteUint16(v uint16) {
	b := convert.Uint16ToBytes(v)
	e.Content = b[0:2]
}

// WriteUint32 to entry content.
func (e *Entry) WriteUint32(v uint32) {
	b := convert.Uint32ToBytes(v)
	e.Content = b[0:4]
}

// WriteUint64 to entry content.
func (e *Entry) WriteUint64(v uint64) {
	b := convert.Uint64ToBytes(v)
	e.Content = b[0:8]
}
