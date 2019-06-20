package leveldb

import (
	"math"

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

func (c *LevelDB) mset(name string, args []resp.Reply) (resp.Reply, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, engine.ErrWrongNumberOfArguments
	}
	if len(args) == 2 {
		return c.set(name, args)
	}

	tran, err := c.db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	defer tran.Commit()

	for i := 0; i != len(args); i += 2 {
		key := toBytes(args[i])
		val := toBytes(args[i+1])
		err := tran.Put(key, val, nil)
		if err != nil {
			return nil, err
		}
	}
	return reply.OK, nil

}

func (c *LevelDB) incr(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 1:
		key := toBytes(args[0])

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		var value int64

		if newVal, _ := tran.Get(key, nil); len(newVal) != 0 {
			value, err = toInteger(newVal)
			if err != nil {
				return nil, err
			}
		}

		value++
		v := toBytes(value)
		err = tran.Put(key, v, nil)
		if err != nil {
			return nil, err
		}
		return resp.ReplyInteger(v), nil
	}
}

func (c *LevelDB) incrby(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		val, err := toInteger(args[1])
		if err != nil {
			return nil, err
		}

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		var value int64
		if newVal, _ := tran.Get(key, nil); len(newVal) != 0 {
			value, err = toInteger(newVal)
			if err != nil {
				return nil, err
			}
		}

		value += val
		v := toBytes(value)
		err = tran.Put(key, v, nil)
		if err != nil {
			return nil, err
		}
		return resp.ReplyInteger(v), nil
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
		defer tran.Commit()

		newVal, _ := tran.Get(key, nil)

		err = tran.Put(key, val, nil)
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
		defer tran.Commit()

		val, err := tran.Get(key, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Delete(key, nil)
		if err != nil {
			return nil, err
		}
		err = tran.Put(newKey, val, nil)
		if err != nil {
			return nil, err
		}

		return reply.OK, nil
	}
}

func (c *LevelDB) del(name string, args []resp.Reply) (resp.Reply, error) {
	tran, err := c.db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	defer tran.Commit()

	keys := make([][]byte, 0, len(args))
	for _, arg := range args {
		key := toBytes(arg)
		val, err := tran.Has(key, nil)
		if err != nil {
			return nil, err
		}
		if val {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		err := tran.Delete(key, nil)
		if err != nil {
			return nil, err
		}
	}
	return resp.ConvertTo(len(keys))
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
	return resp.ConvertTo(sum)
}

func (c *LevelDB) keys(name string, args []resp.Reply) (resp.Reply, error) {

	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
	}
	size, err := toInteger(args[2])
	if err != nil {
		return nil, err
	}
	urange := &util.Range{}

	start := toBytes(args[0])
	limit := toBytes(args[1])
	if len(start) != 0 {
		urange.Start = bytesNext(start)
	}
	if len(limit) != 0 {
		urange.Limit = bytesNext(limit)
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

	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
	}
	size, err := toInteger(args[2])
	if err != nil {
		return nil, err
	}
	urange := &util.Range{}

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

	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
	}
	size, err := toInteger(args[2])
	if err != nil {
		return nil, err
	}
	urange := &util.Range{}

	start := toBytes(args[0])
	limit := toBytes(args[1])
	if len(start) != 0 {
		urange.Start = bytesNext(start)
	}
	if len(limit) != 0 {
		urange.Limit = bytesNext(limit)
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

	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
	}
	size, err := toInteger(args[2])
	if err != nil {
		return nil, err
	}
	urange := &util.Range{}

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

	start := int64(0)
	end := int64(math.MaxInt64 - 1)
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
		start0, err := toInteger(args[1])
		if err != nil {
			return nil, err
		}
		start = start0
		end0, err := toInteger(args[2])
		if err != nil {
			return nil, err
		}
		end = end0
	case 1:
		// No action
	}

	key := toBytes(args[0])
	val, err := c.db.Get(key, nil)
	if err != nil {
		return nil, err
	}

	if start > end {
		return reply.Zero, nil
	}

	if int64(len(val)) > end+1 {
		val = val[:end+1]
	}

	if int64(len(val)) > start {
		val = val[start:]
	}

	var sum uint64
	for _, v := range val {
		for i := 0; i != 8; i++ {
			if v&getBit(i) != 0 {
				sum++
			}
		}
	}
	return resp.ConvertTo(sum)
}

func (c *LevelDB) getbit(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		key := toBytes(args[0])
		offset, err := toInteger(args[1])
		if err != nil {
			return nil, err
		}
		if offset < 0 {
			return reply.Zero, nil
		}
		val, err := c.db.Get(key, nil)
		if err != nil {
			return reply.Zero, nil
		}
		index := offset / 8

		if int64(len(val)) <= index {
			return reply.Zero, nil
		}

		off := getBit(int(offset % 8))

		if val[index]&off == 0 {
			return reply.Zero, nil
		}
		return reply.One, nil
	}
}

func (c *LevelDB) setbit(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:
		key := toBytes(args[0])
		offset, err := toInteger(args[1])
		if err != nil {
			return nil, err
		}
		if offset < 0 {
			return reply.Zero, nil
		}
		flag, err := toInteger(args[2])
		if err != nil {
			return nil, err
		}
		newflage := flag != 0

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		val, _ := tran.Get(key, nil)

		index := offset / 8
		if s := 1 + index - int64(len(val)); s > 0 {
			val = append(val, make([]byte, s)...)
		}

		off := getBit(int(offset % 8))

		oldflag := val[index]&off != 0
		if newflage == oldflag {
			tran.Discard()
			if oldflag {
				return reply.One, nil
			}
			return reply.Zero, nil
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
		if oldflag {
			return reply.One, nil
		}
		return reply.Zero, nil
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
		defer tran.Commit()

		val, _ := tran.Get(key, nil)
		val = append(val, str...)
		err = tran.Put(key, val, nil)
		if err != nil {
			return nil, err
		}

		return resp.ConvertTo(len(val))
	}
}

func (c *LevelDB) strlen(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 1:
		key := toBytes(args[0])
		val, _ := c.db.Get(key, nil)
		return resp.ConvertTo(len(val))
	}
}
