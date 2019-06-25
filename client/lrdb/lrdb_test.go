package lrdb

import (
	"testing"

	"github.com/wzshiming/lrdb"
	"github.com/wzshiming/lrdb/engine/leveldb"
)

var testAddress = "127.0.0.1:60101"

func init() {
	db, err := leveldb.NewLevelDBWithMemStorage()
	if err != nil {
		panic(err)
	}

	go func() {
		err := lrdb.NewLRDB(db.Cmd()).Listen(testAddress)
		if err != nil {
			panic(err)
		}
	}()
}

func TestClient(t *testing.T) {

	cli, err := NewClient(testAddress)
	if err != nil {
		t.Error(err)
		return
	}
	key := "hello"

	t.Log(cli.Exists(key))
	t.Log(cli.Set(key, "world"))
	t.Log(cli.Get(key))
	t.Log(cli.Rename(key, key+"2"))
	t.Log(cli.Keys("", "", 10))
	cli.Close()
}
