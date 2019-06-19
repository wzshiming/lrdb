package engine

import (
	"errors"
)

var (
	ErrWrongNumberOfArguments = errors.New("Error wrong number of arguments")
	ErrUnsupportedForm        = errors.New("Error unsupported form")
	ErrEmptyData              = errors.New("Error empty data")
)
