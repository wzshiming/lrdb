package engine

import (
	"time"

	"github.com/wzshiming/lrdb/reply"
	"github.com/wzshiming/resp"
)

func (c *Commands) cmdEcho(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		return args[0], nil
	}
}

func (c *Commands) cmdPing(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		return args[0], nil
	case 0:
		return reply.PONG, nil
	}
}

func (c *Commands) cmdQuit(name string, args []resp.Reply) (resp.Reply, error) {
	return reply.OK, nil
}

func (c *Commands) cmdTime(name string, args []resp.Reply) (resp.Reply, error) {
	un := time.Now()
	nano := int64(un.Nanosecond())
	unix := un.Unix()

	return resp.ConvertTo([]int64{
		unix,
		nano,
	})
}

func (c *Commands) Registe() {
	c.AddCommand("echo", c.cmdEcho)
	c.AddCommand("ping", c.cmdPing)
	c.AddCommand("quit", c.cmdQuit)
	c.AddCommand("time", c.cmdTime)
}
