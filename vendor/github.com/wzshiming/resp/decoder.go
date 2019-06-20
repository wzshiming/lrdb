package resp

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"unsafe"
)

// Decoder It's a resp decoder
type Decoder struct {
	reader *bufio.Reader
}

// NewDecoder Create a new resp decoder.
func NewDecoder(reader io.Reader) *Decoder {
	bufread, ok := reader.(*bufio.Reader)
	if !ok {
		bufread = bufio.NewReader(reader)
	}
	p := &Decoder{
		reader: bufread,
	}
	return p
}

// Encode returns decode the reply from reader.
func (r *Decoder) Decode() (Reply, error) {
	return r.decodeData()
}

func (r *Decoder) decodeData() (Reply, error) {
	ident, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch ident {
	case MultiBulk:
		return r.decodeMultiBulk()
	case Bulk:
		return r.decodeBulk()
	case Integer:
		return r.decodeInteger()
	case Status:
		return r.decodeStatus()
	case Error:
		return r.decodeError()
	}
	r.reader.UnreadByte()
	e, err := r.decodeInline()
	if err != nil {
		return nil, err
	}
	if e != nil {
		return e, nil
	}
	return r.decodeData()
}

func (r *Decoder) decodeInline() (Reply, error) {
	line, err := r.decodeLine()
	if err != nil {
		return nil, err
	}

	data := ReplyMultiBulk{}
	for len(line) != 0 {
		if line[0] == ' ' {
			line = line[1:]
			continue
		}
		i := bytes.IndexByte(line, ' ')
		if i == -1 {
			data = append(data, ReplyBulk(line))
			break
		}
		data = append(data, ReplyBulk(line[:i]))
		line = line[i+1:]
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func (r *Decoder) decodeLine() ([]byte, error) {
	line, _, err := r.reader.ReadLine()
	if err != nil {
		return nil, err
	}
	return line, nil
}

func (r *Decoder) decodeInt64() (int64, error) {
	line, err := r.decodeLine()
	if err != nil {
		return 0, err
	}
	numLine := *(*string)(unsafe.Pointer(&line))
	return strconv.ParseInt(numLine, 10, 64)
}

func (r *Decoder) decodeMultiBulk() (Reply, error) {
	num, err := r.decodeInt64()
	if err != nil {
		return nil, err
	}
	if num < 0 {
		// The returned interface is not nil but the data inside is nil
		return (ReplyMultiBulk)(nil), nil
	}
	data := make(ReplyMultiBulk, 0, num)
	for i := int64(0); i != num; i++ {
		row, err := r.decodeData()
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, nil
}

func (r *Decoder) decodeError() (Reply, error) {
	line, err := r.decodeLine()
	if err != nil {
		return nil, err
	}
	return ReplyError(line), nil
}

func (r *Decoder) decodeStatus() (Reply, error) {
	line, err := r.decodeLine()
	if err != nil {
		return nil, err
	}
	return ReplyStatus(line), nil
}

func (r *Decoder) decodeInteger() (Reply, error) {
	line, err := r.decodeLine()
	if err != nil {
		return nil, err
	}
	return ReplyInteger(line), nil
}

func (r *Decoder) decodeBulk() (Reply, error) {
	num, err := r.decodeInt64()
	if err != nil {
		return nil, err
	}
	if num < 0 {
		// The returned interface is not nil but the data inside is nil
		return (ReplyBulk)(nil), nil
	}
	buf := make([]byte, num)
	_, err = io.ReadAtLeast(r.reader, buf, int(num))
	if err != nil {
		return nil, err
	}
	_, err = r.decodeLine()
	if err != nil {
		return nil, err
	}
	return ReplyBulk(buf), nil
}
