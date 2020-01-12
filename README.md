# LRDB - A fast NoSQL database for storing big data

[![Build Status](https://travis-ci.org/wzshiming/lrdb.svg?branch=master)](https://travis-ci.org/wzshiming/lrdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/wzshiming/lrdb)](https://goreportcard.com/report/github.com/wzshiming/lrdb)
[![GoDoc](https://godoc.org/github.com/wzshiming/lrdb?status.svg)](https://godoc.org/github.com/wzshiming/lrdb)
[![GitHub license](https://img.shields.io/github/license/wzshiming/lrdb.svg)](https://github.com/wzshiming/lrdb/blob/master/LICENSE)

## Features

* Is a high performace key-value NoSQL database.
* LevelDB client-server support. Redis-protocol frontend to Google's LevelDB backend.
* This is not a SQL database. It does not have a relational data model, it does not support SQL queries, and it has no support for indexes.
* Implemented in golang, it supports all golang supported platforms and architectures.

## Usage

``` sh
$ go get -v github.com/wzshiming/lrdb/cmd/lrdb

$ nohup lrdb -port :10008 -path ./data &

$ go get -v github.com/wzshiming/resp/cmd/resp

$ resp 127.0.0.1:10008

RESP 127.0.0.1:10008> set foo bar
(Status) OK
(1ms)
RESP 127.0.0.1:10008> get foo
"bar"
(1ms)
RESP 127.0.0.1:10008>

```

## License

Pouch is licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/lrdb/blob/master/LICENSE) for the full license text.
