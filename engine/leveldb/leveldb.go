package leveldb

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/wzshiming/resp"
)

var (
	ErrWrongNumberOfArguments = errors.New("Error wrong number of arguments")
	ErrUnsupportedForm        = errors.New("Error unsupported form")
	ErrEmptyData              = errors.New("Error empty data")
)

var (
	OK = resp.ReplyStatus("OK")
)

type LevelDB struct {
	db     *leveldb.DB
	method map[string]func(args []resp.Reply) (resp.Reply, error)
}

func NewLevelDB(path string) (*LevelDB, error) {
	db, err := leveldb.RecoverFile(path, nil)
	if err != nil {
		return nil, err
	}
	c := &LevelDB{
		db:     db,
		method: map[string]func(args []resp.Reply) (resp.Reply, error){},
	}
	c.init()
	return c, nil
}

func (c *LevelDB) init() {
	c.method["get"] = c.get
	c.method["set"] = c.set
	c.method["del"] = c.del
	c.method["exists"] = c.exists
}

func (c *LevelDB) Cmd(r resp.Reply) (resp.Reply, error) {
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

func (c *LevelDB) cmd(args []resp.Reply) (resp.Reply, error) {
	switch t := args[0].(type) {
	default:
		return nil, ErrUnsupportedForm
	case resp.ReplyBulk:
		command := *(*string)(unsafe.Pointer(&t))
		fun, ok := c.method[command]
		if !ok {
			return nil, fmt.Errorf("Error Unknown Command '%s'", command)
		}
		return fun(args[1:])
	}
}

func (c *LevelDB) get(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		val, err := c.db.Get(toBytes(args[0]), nil)
		if err != nil {
			return nil, err
		}
		return resp.ReplyBulk(val), nil
	}
}

func (c *LevelDB) get(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		val, err := c.db.Get(toBytes(args[0]), nil)
		if err != nil {
			return nil, err
		}
		return resp.ReplyBulk(val), nil
	}
}

func (c *LevelDB) set(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 2:
		err := c.db.Put(toBytes(args[0]), toBytes(args[1]), nil)
		if err != nil {
			return nil, err
		}
		return OK, nil
	}
}

func (c *LevelDB) del(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	case 0:
		return nil, ErrWrongNumberOfArguments
	default:
		for _, arg := range args {
			err := c.db.Delete(toBytes(arg), nil)
			if err != nil {
				return nil, err
			}
		}
		return resp.Convert(len(args)), nil
	}
}

func (c *LevelDB) exists(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		val, err := c.db.Has(toBytes(args[0]), nil)
		if err != nil {
			return nil, err
		}

		return resp.Convert(val), nil
	}
}

func toBytes(r resp.Reply) []byte {
	switch t := r.(type) {
	case resp.ReplyBulk:
		return []byte(t)
	case resp.ReplyInteger:
		return []byte(t)
	default:
		return nil
	}

}
