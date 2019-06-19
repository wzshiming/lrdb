package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/wzshiming/lrdb/engine"
	"github.com/wzshiming/lrdb/reply"
	"github.com/wzshiming/resp"
)

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
		return reply.OK, nil
	}
}

func (c *LevelDB) getset(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		val := toBytes(args[1])
		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		newVal, err := tran.Get(key, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Commit()
		if err != nil {
			return nil, err
		}
		return resp.ReplyBulk(newVal), nil
	}
}

func (c *LevelDB) rename(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		newKey := toBytes(args[1])
		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		val, err := tran.Get(key, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Put(newKey, val, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Delete(key, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Commit()
		if err != nil {
			return nil, err
		}
		return reply.OK, nil
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
	defer iter.Release()

	if !iter.First() {
		return multiBulk, nil
	}

	for i := int64(0); i != size; i++ {
		key := cloneBytes(iter.Key())
		multiBulk = append(multiBulk, resp.ReplyBulk(key))
		if !iter.Next() {
			break
		}
	}

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
	defer iter.Release()

	if !iter.Last() {
		return multiBulk, nil
	}

	for i := int64(0); i != size; i++ {
		key := cloneBytes(iter.Key())
		multiBulk = append(multiBulk, resp.ReplyBulk(key))
		if !iter.Prev() {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
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
	defer iter.Release()

	if !iter.First() {
		return multiBulk, nil
	}

	for i := int64(0); i != size; i++ {
		data := cloneBytes(iter.Key())
		multiBulk = append(multiBulk, resp.ReplyBulk(data))
		data = cloneBytes(iter.Value())
		multiBulk = append(multiBulk, resp.ReplyBulk(data))
		if !iter.Next() {
			break
		}
	}

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
	defer iter.Release()

	if !iter.Last() {
		return multiBulk, nil
	}

	for i := int64(0); i != size; i++ {
		data := cloneBytes(iter.Key())
		multiBulk = append(multiBulk, resp.ReplyBulk(data))
		data = cloneBytes(iter.Value())
		multiBulk = append(multiBulk, resp.ReplyBulk(data))
		if !iter.Prev() {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return multiBulk, nil
}

func (c *LevelDB) bitcount(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 1:
		key := toBytes(args[0])
		val, err := c.db.Get(key, nil)
		if err != nil {
			return zero, nil
		}

		var sum uint64
		for _, v := range val {
			for i := 0; i != 8; i++ {
				if v&getBit(i) != 0 {
					sum++
				}
			}
		}
		return resp.Convert(sum), nil
	}
}

func (c *LevelDB) getbit(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		offset := toInteger(args[1])
		if offset < 0 {
			return zero, nil
		}
		val, err := c.db.Get(key, nil)
		if err != nil {
			return zero, nil
		}
		index := offset / 8

		if int64(len(val)) <= index {
			return zero, nil
		}

		off := getBit(int(offset % 8))

		if val[index]&off == 0 {
			return zero, nil
		}
		return one, nil
	}
}

func (c *LevelDB) setbit(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
		key := toBytes(args[0])
		offset := toInteger(args[1])
		if offset < 0 {
			return zero, nil
		}
		flag := toInteger(args[2])
		newflage := flag != 0

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		val, _ := tran.Get(key, nil)

		index := offset / 8
		if s := 1 + index - int64(len(val)); s > 0 {
			val = append(val, make([]byte, s)...)
		}

		off := getBit(int(offset % 8))

		oldflag := val[index]&off != 0
		if newflage == oldflag {
			tran.Discard()
			return zero, nil
		}
		if newflage {
			val[index] |= off
		} else {
			val[index] &= ^off
		}
		err = tran.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Commit()
		if err != nil {
			return nil, err
		}
		return one, nil
	}
}

func (c *LevelDB) append(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		str := toBytes(args[1])

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		val, _ := tran.Get(key, nil)
		val = append(val, str...)
		err = tran.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Commit()
		if err != nil {
			return nil, err
		}
		return one, nil
	}
}

func (c *LevelDB) strlen(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 1:
		key := toBytes(args[0])
		val, _ := c.db.Get(key, nil)
		return resp.Convert(len(val)), nil
	}
}
