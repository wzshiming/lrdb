package lrdb

import (
	"io"
	"net"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/wzshiming/resp"
	"gopkg.in/ffmt.v1"
)

type LRDB struct {
	db *leveldb.DB
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
			ffmt.Mark(err)
			return err
		}
		err := encoder.Encode(reply)
		if err != nil {
			conn.Close()
			ffmt.Mark(err)
			return err
		}
	}

	return nil
}
