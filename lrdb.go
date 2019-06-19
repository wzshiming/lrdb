package lrdb

import (
	"io"
	"net"

	"github.com/wzshiming/resp"
	"gopkg.in/ffmt.v1"
)

type LRDB struct {
	engine Engine
}

func NewLRDB(engine Engine) *LRDB {
	return &LRDB{
		engine: engine,
	}
}

func (db *LRDB) Listen(address string) error {
	listen, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		conn, err := listen.Accept()
		if err != nil {
			ffmt.Mark(err)
			continue
		}
		go db.Handle(conn)
	}

	return nil
}

func (db *LRDB) Handle(conn io.ReadWriteCloser) error {
	decoder := resp.NewDecoder(conn)
	encoder := resp.NewEncoder(conn)

	for {
		reply, err := decoder.Decode()
		if err != nil {
			conn.Close()
			return err
		}

		result, err := db.engine.RawCmd(reply)
		if err != nil {
			result = resp.ReplyError(err.Error())
		}

		err = encoder.Encode(result)
		if err != nil {
			conn.Close()
			return err
		}
	}

	return nil
}
