package leveldb

import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	c.method["info"] = c.info
	c.method["keys"] = c.keys
	c.method["rkeys"] = c.rkeys
	c.method["scan"] = c.scan
	c.method["rscan"] = c.rscan
	c.method["get"] = c.get
	c.method["set"] = c.set
	c.method["getset"] = c.getset
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

func (c *LevelDB) info(args []resp.Reply) (resp.Reply, error) {
	stats := &leveldb.DBStats{}
	err := c.db.Stats(stats)
	if err != nil {
		return nil, err
	}
	return resp.Convert(stats), nil
}

func (c *LevelDB) get(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		key := toBytes(args[0])
		val, err := c.db.Get(key, nil)
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
		key := toBytes(args[0])
		val := toBytes(args[1])
		err := c.db.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
		return OK, nil
	}
}

func (c *LevelDB) getset(args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		val := toBytes(args[1])
		newVal, err := c.db.Get(key, nil)
		if err != nil {
			return nil, err
		}
		err = c.db.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
		return resp.ReplyBulk(newVal), nil
	}
}

func (c *LevelDB) del(args []resp.Reply) (resp.Reply, error) {
	for _, arg := range args {
		err := c.db.Delete(toBytes(arg), nil)
		if err != nil {
			return nil, err
		}
	}
	return resp.Convert(len(args)), nil
}

func (c *LevelDB) exists(args []resp.Reply) (resp.Reply, error) {
	sum := 0
	for _, arg := range args {
		val, err := c.db.Has(toBytes(arg), nil)
		if err != nil {
			return nil, err
		}
		if val {
			sum++
		}
	}
	return resp.Convert(sum), nil
}

func (c *LevelDB) keys(args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 3:
		size = toInteger(args[2])
		fallthrough
	case 2:
		start := toBytes(args[0])
		limit := toBytes(args[1])
		if len(start) != 0 {
			urange.Start = bytesNext(start)
		}
		if len(limit) != 0 {
			urange.Limit = bytesNext(limit)
		}
	}
	multiBulk := resp.ReplyMultiBulk{}
	if size == 0 {
		return multiBulk, nil
	}

	iter := c.db.NewIterator(urange, nil)
	for i := int64(0); i != size && iter.Next(); i++ {
		key := cloneBytes(iter.Key())
		if i == 0 && bytes.Equal(urange.Start, key) {
			continue
		}
		multiBulk = append(multiBulk, resp.ReplyBulk(key))
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return multiBulk, nil
}

func (c *LevelDB) rkeys(args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 3:
		size = toInteger(args[2])
		fallthrough
	case 2:
		start := toBytes(args[1])
		limit := toBytes(args[0])
		if len(start) != 0 {
			urange.Start = start
		}
		if len(limit) != 0 {
			urange.Limit = limit
		}
		if len(start)+len(limit) == 0 {
			urange = nil
		}
	}
	multiBulk := resp.ReplyMultiBulk{}
	if size == 0 {
		return multiBulk, nil
	}
	iter := c.db.NewIterator(urange, nil)
	if iter.Last() {
		for i := int64(0); i != size; i++ {
			key := cloneBytes(iter.Key())
			multiBulk = append(multiBulk, resp.ReplyBulk(key))
			if !iter.Prev() {
				break
			}
		}
		iter.Release()
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}
	return multiBulk, nil
}

func (c *LevelDB) scan(args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 3:
		size = toInteger(args[2])
		fallthrough
	case 2:
		start := toBytes(args[0])
		limit := toBytes(args[1])
		if len(start) != 0 {
			urange.Start = bytesNext(start)
		}
		if len(limit) != 0 {
			urange.Limit = bytesNext(limit)
		}
	}
	multiBulk := resp.ReplyMultiBulk{}
	if size == 0 {
		return multiBulk, nil
	}

	iter := c.db.NewIterator(urange, nil)
	for i := int64(0); i != size && iter.Next(); i++ {
		key := cloneBytes(iter.Value())
		if i == 0 && bytes.Equal(urange.Start, key) {
			continue
		}
		multiBulk = append(multiBulk, resp.ReplyBulk(key))
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return multiBulk, nil
}

func (c *LevelDB) rscan(args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 3:
		size = toInteger(args[2])
		fallthrough
	case 2:
		start := toBytes(args[1])
		limit := toBytes(args[0])
		if len(start) != 0 {
			urange.Start = start
		}
		if len(limit) != 0 {
			urange.Limit = limit
		}
		if len(start)+len(limit) == 0 {
			urange = nil
		}
	}
	multiBulk := resp.ReplyMultiBulk{}
	if size == 0 {
		return multiBulk, nil
	}
	iter := c.db.NewIterator(urange, nil)
	if iter.Last() {
		for i := int64(0); i != size; i++ {
			key := cloneBytes(iter.Value())
			multiBulk = append(multiBulk, resp.ReplyBulk(key))
			if !iter.Prev() {
				break
			}
		}
		iter.Release()
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}
	return multiBulk, nil
}
