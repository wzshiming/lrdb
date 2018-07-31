package engine

import (
	"errors"

	"github.com/wzshiming/resp"
)

var (
	ErrWrongNumberOfArguments = errors.New("Error wrong number of arguments")
	ErrUnsupportedForm        = errors.New("Error unsupported form")
	ErrEmptyData              = errors.New("Error empty data")
)

var (
	OK   = resp.ReplyStatus("OK")
	PONG = resp.ReplyStatus("PONG")
)
