package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/wzshiming/lrdb"
	"github.com/wzshiming/lrdb/engine/leveldb"
	"gopkg.in/ffmt.v1"
)

var debug = flag.String("debug", ":10010", "debug port")
var port = flag.String("port", ":10008", "Listen port")
var path = flag.String("path", "./data", "Data path")

func main() {

	flag.Parse()
	if *debug != "" {
		go http.ListenAndServe(*debug, nil)
	}
	db, err := leveldb.NewLevelDB(*path)
	if err != nil {
		ffmt.Mark(err)
		return
	}
	err = lrdb.NewLRDB(db.Cmd()).Listen(*port)
	ffmt.Mark(err)
}
