package main

import (
	"flag"

	"github.com/wzshiming/lrdb"
	"github.com/wzshiming/lrdb/engine/leveldb"
	"gopkg.in/ffmt.v1"
)

var port = flag.String("port", ":10008", "Listen port")
var path = flag.String("path", "./data", "Data path")

func main() {
	flag.Parse()
	db, err := leveldb.NewLevelDB(*path)
	if err != nil {
		ffmt.Mark(err)
		return
	}
	err = lrdb.NewLRDB(db.Cmd()).Listen(*port)
	ffmt.Mark(err)
}
