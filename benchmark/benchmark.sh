#!/bin/sh

if [[ -z "${DB_ARGS}" ]]; then
    DB_ARGS="redis:alpine;6379 wzshiming/ssdb;8888 wzshiming/lrdb;10008"
fi

if [[ -z "${COMMAND_ARGS}" ]]; then
    COMMAND_ARGS="SET GET"
fi

if [[ -z "${PIPELINE_SIZE_ARGS}" ]]; then
    PIPELINE_SIZE_ARGS="1 2 4 8 16"
fi

if [[ -z "${CLIENT_SIZE_ARGS}" ]]; then
    CLIENT_SIZE_ARGS="50 100 200"
fi

if [[ -z "${REQUEST_SIZE_ARGS}" ]]; then
    REQUEST_SIZE_ARGS="100000 1000000"
fi

echo "Start benchmark ... "
for COMMAND in ${COMMAND_ARGS[@]}; do
    for PIPELINE_SIZE in ${PIPELINE_SIZE_ARGS[@]}; do
        for CLIENT_SIZE in ${CLIENT_SIZE_ARGS[@]}; do
            for REQUEST_SIZE in ${REQUEST_SIZE_ARGS[@]}; do
                for DB in ${DB_ARGS[@]}; do
                    DB_IMAGE=${DB%;*}
                    DB_PORT=${DB#*;}
                    docker run --rm -d --name lrdb-tmp-benchmark $DB_IMAGE > /dev/null 2>&1
                    echo "Image:$DB_IMAGE;	Test:$COMMAND;	Clients:$CLIENT_SIZE;	Requests:$REQUEST_SIZE;	Pipeline:$PIPELINE_SIZE;"
                    docker run --rm -it --link lrdb-tmp-benchmark:lrdb-tmp-benchmark redis:alpine \
                        redis-benchmark -h lrdb-tmp-benchmark -p $DB_PORT -q \
                        -t $COMMAND -c $CLIENT_SIZE -n $REQUEST_SIZE -P $PIPELINE_SIZE
                    docker kill lrdb-tmp-benchmark > /dev/null 2>&1
                done
            done
        done
    done
done
echo "Finished benchmark."
