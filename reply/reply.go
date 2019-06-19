package reply

import (
	"github.com/wzshiming/resp"
)

var (
	OK   = resp.ReplyStatus("OK")
	PONG = resp.ReplyStatus("PONG")
	Zero = resp.ReplyInteger("0")
	One  = resp.ReplyInteger("1")
)
