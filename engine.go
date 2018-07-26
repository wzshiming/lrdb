package lrdb

import (
	"github.com/wzshiming/resp"
)

type Engine interface {
	Cmd(resp.Reply) (resp.Reply, error)
}

type Echo struct{}

func (Echo) Cmd(r resp.Reply) (resp.Reply, error) {
	return r, nil
}
