package resp

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

// The signature of the RESP protocol
const (
	Status    = '+'
	Error     = '-'
	Integer   = ':'
	Bulk      = '$'
	MultiBulk = '*'
)

var (
	crlf   = []byte{'\r', '\n'}
	nilVal = []byte{'-', '1'}
)

// Reply is Data kind interface
type Reply interface {
	replyNode()
	Format(level uint8) string
}

// ReplyBulk Is could be any data of reply
type ReplyBulk []byte

// ReplyMultiBulk Is an array of multiple reply
type ReplyMultiBulk []Reply

// ReplyInteger is has to be integer of reply
type ReplyInteger []byte

// ReplyStatus is has to be a row of status reply
type ReplyStatus []byte

// ReplyError is has to be a row of error reply
type ReplyError []byte

func (r ReplyBulk) replyNode()      {}
func (r ReplyMultiBulk) replyNode() {}
func (r ReplyInteger) replyNode()   {}
func (r ReplyStatus) replyNode()    {}
func (r ReplyError) replyNode()     {}

func (r ReplyBulk) Format(level uint8) string {
	return strconv.Quote(*(*string)(unsafe.Pointer(&r)))
}
func (r ReplyMultiBulk) Format(level uint8) string {
	if len(r) == 0 {
		return ""
	}
	ss := make([]string, 0, len(r))
	lev := strings.Repeat("   ", int(level))

	gsp := int(math.Log10(math.Max(float64(len(r)-1), 1))) + 1
	for i, v := range r {
		text := v.Format(level + 1)
		le := lev
		if i == 0 {
			le = ""
		}

		spr := strings.Repeat(" ", gsp-int(math.Log10(math.Max(float64(i), 1))))

		ss = append(ss, fmt.Sprintf("%s%d)%s%s", le, i, spr, text))
	}
	out := strings.Join(ss, "\n")
	return out
}
func (r ReplyInteger) Format(level uint8) string {
	text := append([]byte("(Integer) "), r...)
	return *(*string)(unsafe.Pointer(&text))
}
func (r ReplyStatus) Format(level uint8) string {
	text := append([]byte("(Status) "), r...)
	return *(*string)(unsafe.Pointer(&text))
}
func (r ReplyError) Format(level uint8) string {
	text := append([]byte("(Error) "), r...)
	return *(*string)(unsafe.Pointer(&text))
}

// Equal returns a boolean reporting whether a and b
// are the same length and contain the same Reply.
func Equal(a, b Reply) bool {
	switch aa := a.(type) {
	default:
		return false
	case ReplyMultiBulk:
		bb, ok := b.(ReplyMultiBulk)
		if !ok {
			return false
		}
		if len(aa) != len(bb) {
			return false
		}
		for i := range aa {
			if !Equal(aa[i], bb[i]) {
				return false
			}
		}
		return true
	case ReplyBulk:
		bb, ok := b.(ReplyBulk)
		if !ok {
			return false
		}
		return bytes.Equal([]byte(aa), []byte(bb))
	case ReplyInteger:
		bb, ok := b.(ReplyInteger)
		if !ok {
			return false
		}
		return bytes.Equal([]byte(aa), []byte(bb))
	case ReplyStatus:
		bb, ok := b.(ReplyStatus)
		if !ok {
			return false
		}
		return bytes.Equal([]byte(aa), []byte(bb))
	case ReplyError:
		bb, ok := b.(ReplyError)
		if !ok {
			return false
		}
		return bytes.Equal([]byte(aa), []byte(bb))
	}

}
