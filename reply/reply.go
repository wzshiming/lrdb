package reply

import (
	"github.com/wzshiming/resp"
)

var (
	OK = resp.ReplyStatus("OK")
)

var (
	PONG = resp.ReplyStatus("PONG")
	PING = resp.ReplyBulk("PING")
)

var (
	Zero = resp.ReplyInteger("0")
	One  = resp.ReplyInteger("1")
)
