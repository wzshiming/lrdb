package engine

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/wzshiming/lrdb"
	"github.com/wzshiming/resp"
)

type Commands struct {
	method map[string]lrdb.Cmd
	ohter  lrdb.Cmd
}

func NewCommands(ohter lrdb.Cmd) *Commands {
	c := &Commands{
		ohter:  ohter,
		method: map[string]lrdb.Cmd{},
	}
	return c
}

func (c *Commands) AddCommand(name string, cmd lrdb.Cmd) {
	c.method[name] = cmd
}

func (c *Commands) RawCmd(r resp.Reply) (resp.Reply, error) {
	switch t := r.(type) {
	default:
		return nil, ErrUnsupportedForm
	case resp.ReplyMultiBulk:
		if len(t) == 0 {
			return nil, ErrEmptyData
		}
		return c.cmd(t[:])
	}
}

func (c *Commands) Cmd(name string, args []resp.Reply) (resp.Reply, error) {
	fun, ok := c.method[name]
	if !ok {
		if c.ohter != nil {
			return c.ohter(name, args)
		}
		return nil, fmt.Errorf("Error Unknown Command '%s'", name)
	}
	return fun(name, args)
}

func (c *Commands) cmd(args []resp.Reply) (resp.Reply, error) {
	switch t := args[0].(type) {
	default:
		return nil, ErrUnsupportedForm
	case resp.ReplyBulk:
		command := *(*string)(unsafe.Pointer(&t))
		command = strings.ToLower(command)
		return c.Cmd(command, args[1:])
	}
}
