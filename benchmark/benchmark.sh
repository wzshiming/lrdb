#!/bin/sh

echo "Start redis benchmark ... "
docker run --rm -d --name lrdb-redis redis:alpine redis-server
docker run --rm -it --name lrdb-benchmark --link lrdb-redis:lrdb-redis redis:alpine redis-benchmark -h lrdb-redis -p 6379 -c 20 -n 1000000 -t set,get -q
docker kill lrdb-redis
echo "Finished redis benchmark"

echo "Start lrdb benchmark ... "
docker run --rm -d --name lrdb-lrdb wzshiming/lrdb
docker run --rm -it --name lrdb-benchmark --link lrdb-lrdb:lrdb-lrdb redis:alpine redis-benchmark -h lrdb-lrdb -p 10008 -c 20 -n 1000000 -t set,get -q
docker kill lrdb-lrdb
echo "Finished lrdb benchmark"