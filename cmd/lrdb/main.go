package main

import (
	"flag"
	"fmt"

	"github.com/wzshiming/lrdb"
	"github.com/wzshiming/lrdb/engine/leveldb"
)

var port = flag.String("port", ":10008", "Listen port")
var path = flag.String("path", "./data", "Data path")

func main() {
	flag.Parse()
	db, err := leveldb.NewLevelDB(*path)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = lrdb.NewLRDB(db.Cmd()).Listen(*port)
	if err != nil {
		fmt.Println(err)
		return
	}
}
