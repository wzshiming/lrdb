package engine

import (
	"strconv"
	"unsafe"
)

func IncrByInt64(data []byte, i int64) ([]byte, int64, error) {
	d, err := ToInt64(data)
	if err != nil {
		return nil, 0, err
	}
	d += i
	return strconv.AppendInt(nil, d, 10), d, nil
}

func ToInt64(data []byte) (int64, error) {
	d, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&data)), 0, 0)
	if err != nil {
		return 0, err
	}
	return d, nil
}

func GetBit(data []byte, offset int64) (bool, error) {
	index := offset / 8

	if int64(len(data)) <= index {
		return false, nil
	}

	off := BitOffset(int(offset % 8))

	if data[index]&off == 0 {
		return false, nil
	}
	return true, nil
}

func SetBit(data []byte, offset int64, b bool) ([]byte, bool, error) {
	index := offset / 8
	if s := 1 + index - int64(len(data)); s > 0 {
		data = append(data, make([]byte, s)...)
	}

	if int64(len(data)) <= index {
		return data, false, nil
	}

	off := BitOffset(int(offset % 8))

	if (data[index]&off == 0) == b {
		return data, false, nil
	}
	if b {
		data[index] |= off
	} else {
		data[index] &^= off
	}
	return data, true, nil
}

var bitcount [256]byte

func init() {
	for i := 0; i != 256; i++ {
		var b byte
		for off := 0; off != 8; off++ {
			if (1<<byte(off))&byte(i) != 0 {
				b++
			}
		}
		bitcount[i] = b
	}
}

func Bitcount(data []byte) (sum int64) {
	for _, v := range data {
		sum += int64(bitcount[v])
	}
	return sum
}

func BitOffset(i int) byte {
	switch i {
	default:
		return 0
	case 0:
		return 1 << 7
	case 1:
		return 1 << 6
	case 2:
		return 1 << 5
	case 3:
		return 1 << 4
	case 4:
		return 1 << 3
	case 5:
		return 1 << 2
	case 6:
		return 1 << 1
	case 7:
		return 1 << 0
	}
}
