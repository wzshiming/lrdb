package lrdb

import (
	"github.com/wzshiming/resp"
)

type Cmd func(name string, args []resp.Reply) (resp.Reply, error)

type Engine interface {
	RawCmd(resp.Reply) (resp.Reply, error)
	Cmd(name string, args []resp.Reply) (resp.Reply, error)
}
