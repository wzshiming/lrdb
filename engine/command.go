package engine

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/wzshiming/lrdb"
	"github.com/wzshiming/resp"
)

type Commands struct {
	method map[string]lrdb.CmdFunc
	ohter  lrdb.CmdFunc
}

func NewCommands(ohter lrdb.CmdFunc) *Commands {
	c := &Commands{
		ohter:  ohter,
		method: map[string]lrdb.CmdFunc{},
	}
	c.registe()
	return c
}

func (c *Commands) AddCommand(name string, cmd lrdb.CmdFunc) {
	c.method[name] = cmd
}

func (c *Commands) Cmd(r resp.Reply) (resp.Reply, error) {
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

func (c *Commands) cmd(args []resp.Reply) (resp.Reply, error) {
	switch t := args[0].(type) {
	default:
		return nil, ErrUnsupportedForm
	case resp.ReplyBulk:
		name := *(*string)(unsafe.Pointer(&t))
		name = strings.ToLower(name)
		args = args[1:]
		fun, ok := c.method[name]
		if !ok {
			if c.ohter != nil {
				return c.ohter(name, args)
			}
			return nil, fmt.Errorf("Error Unknown Command '%s'", name)
		}
		return fun(name, args)
	}
}
