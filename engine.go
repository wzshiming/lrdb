package lrdb

import (
	"github.com/wzshiming/resp"
)

type CmdFunc func(name string, args []resp.Reply) (resp.Reply, error)

type Engine interface {
	Cmd(resp.Reply) (resp.Reply, error)
}
