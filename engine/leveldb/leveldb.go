package leveldb

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/wzshiming/lrdb/engine"
	"github.com/wzshiming/resp"
)

type LevelDB struct {
	db *leveldb.DB
}

func NewLevelDB(path string) (*LevelDB, error) {
	db, err := leveldb.RecoverFile(path, nil)
	if err != nil {
		return nil, err
	}
	c := &LevelDB{
		db: db,
	}
	return c, nil
}

func (c *LevelDB) Cmd() *engine.Commands {
	commands := engine.NewCommands(nil)
	engine.Registe(commands)
	commands.AddCommand("info", c.info)
	commands.AddCommand("keys", c.keys)
	commands.AddCommand("rkeys", c.rkeys)
	commands.AddCommand("scan", c.scan)
	commands.AddCommand("rscan", c.rscan)
	commands.AddCommand("get", c.get)
	commands.AddCommand("set", c.set)
	commands.AddCommand("getset", c.getset)
	commands.AddCommand("del", c.del)
	commands.AddCommand("exists", c.exists)
	return commands
}

func (c *LevelDB) info(name string, args []resp.Reply) (resp.Reply, error) {
	stats := &leveldb.DBStats{}
	err := c.db.Stats(stats)
	if err != nil {
		return nil, err
	}
	return resp.Convert(stats), nil
}

func (c *LevelDB) get(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 1:
		key := toBytes(args[0])
		val, err := c.db.Get(key, nil)
		if err != nil {
			return nil, err
		}
		return resp.ReplyBulk(val), nil
	}
}

func (c *LevelDB) set(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		val := toBytes(args[1])
		err := c.db.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
		return engine.OK, nil
	}
}

func (c *LevelDB) getset(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
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

func (c *LevelDB) del(name string, args []resp.Reply) (resp.Reply, error) {
	for _, arg := range args {
		err := c.db.Delete(toBytes(arg), nil)
		if err != nil {
			return nil, err
		}
	}
	return resp.Convert(len(args)), nil
}

func (c *LevelDB) exists(name string, args []resp.Reply) (resp.Reply, error) {
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

func (c *LevelDB) keys(name string, args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
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

func (c *LevelDB) rkeys(name string, args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
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

func (c *LevelDB) scan(name string, args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
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

func (c *LevelDB) rscan(name string, args []resp.Reply) (resp.Reply, error) {
	urange := &util.Range{}
	size := int64(-1)
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
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
