#!/bin/sh

echo "Start redis benchmark ... "
docker run --rm -d --name lrdb-redis redis:alpine redis-server
docker run --rm -it --link lrdb-redis:lrdb-redis redis:alpine redis-benchmark -h lrdb-redis -p 6379 -q -c 50 -n 100000 -t ping,set,get
docker run --rm -it --link lrdb-redis:lrdb-redis redis:alpine redis-benchmark -h lrdb-redis -p 6379 -q -c 50 -n 100000 -t ping,set,get -P 4
docker run --rm -it --link lrdb-redis:lrdb-redis redis:alpine redis-benchmark -h lrdb-redis -p 6379 -q -c 50 -n 100000 -t ping,set,get -P 8
docker kill lrdb-redis
echo "Finished redis benchmark"

echo "Start lrdb benchmark ... "
docker run --rm -d --name lrdb-lrdb wzshiming/lrdb
docker run --rm -it --link lrdb-lrdb:lrdb-lrdb redis:alpine redis-benchmark -h lrdb-lrdb -p 10008 -q -c 50 -n 100000 -t ping,set,get
docker run --rm -it --link lrdb-lrdb:lrdb-lrdb redis:alpine redis-benchmark -h lrdb-lrdb -p 10008 -q -c 50 -n 100000 -t ping,set,get -P 4
docker run --rm -it --link lrdb-lrdb:lrdb-lrdb redis:alpine redis-benchmark -h lrdb-lrdb -p 10008 -q -c 50 -n 100000 -t ping,set,get -P 8
docker kill lrdb-lrdb
echo "Finished lrdb benchmark"
