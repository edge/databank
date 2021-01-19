package endian

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// HostEndian attempts to identify the native endianness of the host machine.
//
// Based on: https://stackoverflow.com/a/53286786/1717753
func HostEndian() (bo binary.ByteOrder, err error) {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		bo = binary.LittleEndian
		break
	case [2]byte{0xAB, 0xCD}:
		bo = binary.BigEndian
		break
	default:
		err = fmt.Errorf("Could not determine native endianness")
	}
	return
}
