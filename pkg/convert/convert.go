package convert

import (
	"encoding/binary"

	"github.com/edge/databank/pkg/endian"
)

func Endian() binary.ByteOrder {
	e, err := endian.HostEndian()
	if err != nil {
		panic(err)
	}
	return e
}

func ToBytes(i interface{}) (o []byte, ok bool) {
	ok = true
	switch i.(type) {
	case []byte:
		o = i.([]byte)
		break
	case int16:
		b := Int16ToBytes(i.(int16))
		o = b[0:2]
		break
	case int32:
		b := Int32ToBytes(i.(int32))
		o = b[0:4]
		break
	case int64:
		b := Int64ToBytes(i.(int64))
		o = b[0:8]
		break
	case string:
		o = StringToBytes(i.(string))
		break
	case uint16:
		b := Uint16ToBytes(i.(uint16))
		o = b[0:2]
		break
	case uint32:
		b := Uint32ToBytes(i.(uint32))
		o = b[0:4]
		break
	case uint64:
		b := Uint64ToBytes(i.(uint64))
		o = b[0:8]
		break
	default:
		ok = false
		break
	}
	return
}

func BytesToInt16(i [2]byte) int16 {
	return int16(Endian().Uint16(i[0:2]))
}

func BytesToInt32(i [4]byte) int32 {
	return int32(Endian().Uint32(i[0:4]))
}

func BytesToInt64(i [8]byte) int64 {
	return int64(Endian().Uint64(i[0:8]))
}

func BytesToString(i []byte) string {
	return string(i)
}

func BytesToUint16(i [2]byte) uint16 {
	return Endian().Uint16(i[0:2])
}

func BytesToUint32(i [4]byte) uint32 {
	return Endian().Uint32(i[0:4])
}

func BytesToUint64(i [8]byte) uint64 {
	return Endian().Uint64(i[0:8])
}

func Int16ToBytes(i int16) (o [2]byte) {
	Endian().PutUint16(o[0:2], uint16(i))
	return
}

func Int32ToBytes(i int32) (o [4]byte) {
	Endian().PutUint32(o[0:4], uint32(i))
	return
}

func Int64ToBytes(i int64) (o [8]byte) {
	Endian().PutUint64(o[0:8], uint64(i))
	return
}

func StringToBytes(i string) []byte {
	return []byte(i)
}

func Uint16ToBytes(i uint16) (o [2]byte) {
	Endian().PutUint16(o[0:2], i)
	return
}

func Uint32ToBytes(i uint32) (o [4]byte) {
	Endian().PutUint32(o[0:4], i)
	return
}

func Uint64ToBytes(i uint64) (o [8]byte) {
	Endian().PutUint64(o[0:8], i)
	return
}
