# LRDB - A fast NoSQL database for storing big data

LRDB is a high performace key-value NoSQL database, an alternative to Redis.

## Features

* A redis-protocol compatible frontend to google's leveldb
* Designed to store collection data
* Persistent key-value storage

## Usage

``` sh
$ go install github.com/wzshiming/lrdb/cmd/lrdb

$ nohup lrdb -port 10008 -path ./data &

$ go install github.com/wzshiming/resp/cmd/resp

$ resp 127.0.0.1:10008

RESP 127.0.0.1:10008> set foo bar
(Status) OK
(2ms)
RESP 127.0.0.1:10008> get foo
"bar"
(2ms)
RESP 127.0.0.1:10008>

```

## License

Pouch is licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/lrdb/blob/master/LICENSE) for the full license text.
