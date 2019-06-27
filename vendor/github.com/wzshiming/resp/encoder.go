package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Encoder It's a resp encoder.
type Encoder struct {
	writer *bufio.Writer
}

// NewEncoder Create a new resp encoder.
func NewEncoder(writer io.Writer) *Encoder {
	bufwrite, ok := writer.(*bufio.Writer)
	if !ok {
		bufwrite = bufio.NewWriter(writer)
	}
	p := &Encoder{
		writer: bufwrite,
	}
	return p
}

// Encode is encode the reply to writer.
func (w *Encoder) Encode(r Reply) error {
	err := w.encodeData(r)
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *Encoder) encodeKind(kind byte) error {
	return w.writer.WriteByte(kind)
}

func (w *Encoder) encodeLine(data []byte) error {
	_, err := w.writer.Write(data)
	if err != nil {
		return err
	}
	_, err = w.writer.Write(crlf)
	if err != nil {
		return err
	}
	return nil
}

func (w *Encoder) encodeData(r Reply) error {
	switch t := r.(type) {
	case ReplyBulk:
		return w.encodeBulk(t)
	case ReplyMultiBulk:
		return w.encodeMultiBulk(t)
	case ReplyInteger:
		return w.encodeInteger(t)
	case ReplyStatus:
		return w.encodeStatus(t)
	case ReplyError:
		return w.encodeError(t)
	}
	return fmt.Errorf("errors protocol: undefined")
}

func (w *Encoder) encodeMultiBulk(r ReplyMultiBulk) error {
	err := w.encodeKind(MultiBulk)
	if err != nil {
		return err
	}

	if r == nil {
		err = w.encodeLine(nilVal)
		if err != nil {
			return err
		}
		return nil
	}
	err = w.encodeLine(strconv.AppendInt(nil, int64(len(r)), 10))
	if err != nil {
		return err
	}
	for _, raw := range r {
		err := w.encodeData(raw)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Encoder) encodeBulk(r ReplyBulk) error {
	err := w.encodeKind(Bulk)
	if err != nil {
		return err
	}
	if r == nil {
		err = w.encodeLine(nilVal)
		if err != nil {
			return err
		}
		return nil
	}

	err = w.encodeLine(strconv.AppendInt(nil, int64(len(r)), 10))
	if err != nil {
		return err
	}
	return w.encodeLine([]byte(r))
}

func (w *Encoder) encodeInteger(r ReplyInteger) error {
	err := w.encodeKind(Integer)
	if err != nil {
		return err
	}
	return w.encodeLine([]byte(r))
}

func (w *Encoder) encodeStatus(r ReplyStatus) error {
	err := w.encodeKind(Status)
	if err != nil {
		return err
	}
	return w.encodeLine([]byte(r))
}

func (w *Encoder) encodeError(r ReplyError) error {
	err := w.encodeKind(Error)
	if err != nil {
		return err
	}
	return w.encodeLine([]byte(r))
}
