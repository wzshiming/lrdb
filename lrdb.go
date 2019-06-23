package lrdb

import (
	"log"
	"net"
	"os"

	"github.com/wzshiming/resp"
)

type LRDB struct {
	engine Engine
	logger *log.Logger
}

func NewLRDB(engine Engine) *LRDB {
	return &LRDB{
		engine: engine,
		logger: log.New(os.Stdout, "[LRDB] ", log.LstdFlags),
	}
}

func (db *LRDB) Listen(address string) error {
	listen, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	db.logger.Println("Listen", address)
	for {
		conn, err := listen.Accept()
		if err != nil {
			db.logger.Println(err)
			continue
		}
		go db.Handle(conn)
	}

	return nil
}

func (db *LRDB) Handle(conn net.Conn) error {
	decoder := resp.NewDecoder(conn)
	encoder := resp.NewEncoder(conn)
	defer conn.Close()
	addr := conn.RemoteAddr()
	db.logger.Println("Join", addr)
	for {
		reply, err := decoder.Decode()
		if err != nil {
			db.logger.Println("Quit", addr, err)
			return err
		}

		result, err := db.engine.Cmd(reply)
		if err != nil {
			result = resp.ReplyError(err.Error())
		}

		err = encoder.Encode(result)
		if err != nil {
			db.logger.Println("Quit", addr, err)
			return err
		}
	}
}
