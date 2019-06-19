package engine

import (
	"time"

	"github.com/wzshiming/resp"
)

func cmdEcho(name string, args []resp.Reply) (resp.Reply, error) {
	return resp.ReplyMultiBulk(args), nil
}

func cmdPing(name string, args []resp.Reply) (resp.Reply, error) {
	return PONG, nil
}

func cmdQuit(name string, args []resp.Reply) (resp.Reply, error) {
	return OK, nil
}

func cmdTime(name string, args []resp.Reply) (resp.Reply, error) {
	un := time.Now()
	nano := int64(un.Nanosecond())
	unix := un.Unix()

	return resp.Convert([]int64{
		unix,
		nano,
	}), nil
}

func Registe(commands *Commands) {
	commands.AddCommand("echo", cmdEcho)
	commands.AddCommand("ping", cmdPing)
	commands.AddCommand("quit", cmdQuit)
	commands.AddCommand("time", cmdTime)
}
