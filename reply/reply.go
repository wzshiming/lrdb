package reply

import (
	"github.com/wzshiming/resp"
)

var (
	OK   = resp.ReplyStatus("OK")
	PONG = resp.ReplyStatus("PONG")
)
