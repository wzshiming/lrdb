package engine

import (
	"github.com/wzshiming/resp"
)

func echo(name string, args []resp.Reply) (resp.Reply, error) {
	return resp.ReplyMultiBulk(args), nil
}

func ping(name string, args []resp.Reply) (resp.Reply, error) {
	return PONG, nil
}

func quit(name string, args []resp.Reply) (resp.Reply, error) {
	return OK, nil
}

func Registe(commands *Commands) {
	commands.AddCommand("echo", echo)
	commands.AddCommand("ping", ping)
	commands.AddCommand("quit", quit)
}

