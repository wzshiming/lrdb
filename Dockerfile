FROM golang:alpine AS builder 

WORKDIR /go/src/github.com/wzshiming/lrdb/
COPY . .
RUN go install ./cmd/lrdb


FROM alpine 
COPY --from=builder /go/bin/lrdb /usr/bin/

EXPOSE 10008
VOLUME [ "/data" ]
ENTRYPOINT [ "lrdb" ]