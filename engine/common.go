package engine

import (
	"time"

	"github.com/wzshiming/lrdb/reply"
	"github.com/wzshiming/resp"
)

func cmdEcho(name string, args []resp.Reply) (resp.Reply, error) {
	return resp.ReplyMultiBulk(args), nil
}

func cmdPing(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return resp.ReplyMultiBulk(args), nil
	case 0:
		return reply.PONG, nil
	}
}

func cmdQuit(name string, args []resp.Reply) (resp.Reply, error) {
	return reply.OK, nil
}

func cmdTime(name string, args []resp.Reply) (resp.Reply, error) {
	un := time.Now()
	nano := int64(un.Nanosecond())
	unix := un.Unix()

	return resp.ConvertTo([]int64{
		unix,
		nano,
	})
}

func Registe(commands *Commands) {
	commands.AddCommand("echo", cmdEcho)
	commands.AddCommand("ping", cmdPing)
	commands.AddCommand("quit", cmdQuit)
	commands.AddCommand("time", cmdTime)
}
