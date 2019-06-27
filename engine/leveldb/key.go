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
		var key []byte
		err := resp.ConvertFrom(args[0], &key)
		if err != nil {
			return nil, err
		}
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
		var key []byte
		var val []byte
		err := resp.ConvertFrom(args[0], &key)
		if err != nil {
			return nil, err
		}
		err = resp.ConvertFrom(args[1], &val)
		if err != nil {
			return nil, err
		}

		err = c.db.Put(key, val, nil)
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
		var key []byte
		var val []byte
		err = resp.ConvertFrom(args[i], &key)
		if err != nil {
			tran.Discard()
			return nil, err
		}
		err = resp.ConvertFrom(args[i+1], &val)
		if err != nil {
			tran.Discard()
			return nil, err
		}

		err = tran.Put(key, val, nil)
		if err != nil {
			tran.Discard()
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

		var key []byte
		err := resp.ConvertFrom(args[0], &key)
		if err != nil {
			return nil, err
		}

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		val, err := tran.Get(key, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}

		val, _, err = engine.IncrByInt64(val, 1)
		if err != nil {
			tran.Discard()
			return nil, err
		}

		err = tran.Put(key, val, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}
		return resp.ReplyInteger(val), nil
	}
}

func (c *LevelDB) incrby(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		var key []byte
		var inc int64
		err := resp.ConvertFrom(args[0], &key)
		if err != nil {
			return nil, err
		}
		err = resp.ConvertFrom(args[1], &inc)
		if err != nil {
			return nil, err
		}

		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		val, err := tran.Get(key, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}

		val, _, err = engine.IncrByInt64(val, inc)
		if err != nil {
			tran.Discard()
			return nil, err
		}

		err = tran.Put(key, val, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}
		return resp.ReplyInteger(val), nil
	}
}

func (c *LevelDB) getset(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
		var key []byte
		var val []byte
		err := resp.ConvertFrom(args[0], &key)
		if err != nil {
			return nil, err
		}
		err = resp.ConvertFrom(args[1], &val)
		if err != nil {
			return nil, err
		}
		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		newVal, _ := tran.Get(key, nil)

		err = tran.Put(key, val, nil)
		if err != nil {
			tran.Discard()
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
		var key []byte
		var newKey []byte
		err := resp.ConvertFrom(args[0], &key)
		if err != nil {
			return nil, err
		}
		err = resp.ConvertFrom(args[1], &newKey)
		if err != nil {
			return nil, err
		}
		tran, err := c.db.OpenTransaction()
		if err != nil {
			return nil, err
		}
		defer tran.Commit()

		val, err := tran.Get(key, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}
		err = tran.Delete(key, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}
		err = tran.Put(newKey, val, nil)
		if err != nil {
			tran.Discard()
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
		var key []byte
		err := resp.ConvertFrom(arg, &key)
		if err != nil {
			return nil, err
		}

		val, err := tran.Has(key, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}
		if val {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		err := tran.Delete(key, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}
	}
	return resp.ConvertTo(len(keys))
}

func (c *LevelDB) exists(name string, args []resp.Reply) (resp.Reply, error) {
	snap, err := c.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	sum := 0
	for _, arg := range args {
		var key []byte
		err := resp.ConvertFrom(arg, &key)
		if err != nil {
			return nil, err
		}

		val, err := snap.Has(key, nil)
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

	var start []byte
	var limit []byte
	var size int64

	err := resp.ConvertFrom(args[0], &start)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[1], &limit)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[2], &size)
	if err != nil {
		return nil, err
	}

	urange := &util.Range{}
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

	snap, err := c.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	iter := snap.NewIterator(urange, nil)
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
	var start []byte
	var limit []byte
	var size int64

	err := resp.ConvertFrom(args[0], &start)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[1], &limit)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[2], &size)
	if err != nil {
		return nil, err
	}

	urange := &util.Range{}
	if len(start) != 0 {
		urange.Start = start
	}
	if len(limit) != 0 {
		urange.Limit = limit
	}

	multiBulk := resp.ReplyMultiBulk{}
	if size == 0 {
		return multiBulk, nil
	}

	snap, err := c.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	iter := snap.NewIterator(urange, nil)
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
	var start []byte
	var limit []byte
	var size int64

	err := resp.ConvertFrom(args[0], &start)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[1], &limit)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[2], &size)
	if err != nil {
		return nil, err
	}

	urange := &util.Range{}
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

	snap, err := c.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	iter := snap.NewIterator(urange, nil)
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
	var start []byte
	var limit []byte
	var size int64

	err := resp.ConvertFrom(args[0], &start)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[1], &limit)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[2], &size)
	if err != nil {
		return nil, err
	}

	urange := &util.Range{}
	if len(start) != 0 {
		urange.Start = start
	}
	if len(limit) != 0 {
		urange.Limit = limit
	}

	multiBulk := resp.ReplyMultiBulk{}
	if size == 0 {
		return multiBulk, nil
	}

	snap, err := c.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	iter := snap.NewIterator(urange, nil)
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
		err := resp.ConvertFrom(args[1], &start)
		if err != nil {
			return nil, err
		}
		err = resp.ConvertFrom(args[2], &end)
		if err != nil {
			return nil, err
		}
	case 1:
		// No action
	}
	var key []byte
	err := resp.ConvertFrom(args[0], &key)
	if err != nil {
		return nil, err
	}

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
	sum := engine.Bitcount(val)
	return resp.ConvertTo(sum)
}

func (c *LevelDB) getbit(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:
	}

	var key []byte
	var offset int64
	err := resp.ConvertFrom(args[0], &key)
	if err != nil {
		return nil, err
	}

	err = resp.ConvertFrom(args[1], &offset)
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

	b, err := engine.GetBit(val, offset)
    if err != nil {
		return reply.Zero, nil
	}
	if b {
		return reply.One, nil
	}
	return reply.Zero, nil
}

func (c *LevelDB) setbit(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 3:

	}

	var key []byte
	var offset int64
	var flag int64
	err := resp.ConvertFrom(args[0], &key)
	if err != nil {
		return nil, err
	}

	err = resp.ConvertFrom(args[1], &offset)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return reply.Zero, nil
	}
	err = resp.ConvertFrom(args[2], &flag)
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

	val, ok, err := engine.SetBit(val, offset, newflage)
	if err != nil {
		return nil, err
	}
	if ok {
		err = tran.Put(key, val, nil)
		if err != nil {
			tran.Discard()
			return nil, err
		}

	}
	if newflage {
		return reply.Zero, nil
	}
	return reply.One, nil
}

func (c *LevelDB) append(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 2:

	}
	var key []byte
	var str []byte

	err := resp.ConvertFrom(args[0], &key)
	if err != nil {
		return nil, err
	}
	err = resp.ConvertFrom(args[1], &str)
	if err != nil {
		return nil, err
	}

	tran, err := c.db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	defer tran.Commit()

	val, _ := tran.Get(key, nil)
	val = append(val, str...)
	err = tran.Put(key, val, nil)
	if err != nil {
		tran.Discard()
		return nil, err
	}

	return resp.ConvertTo(len(val))
}

func (c *LevelDB) strlen(name string, args []resp.Reply) (resp.Reply, error) {
	switch len(args) {
	default:
		return nil, engine.ErrWrongNumberOfArguments
	case 1:
	}

	var key []byte
	err := resp.ConvertFrom(args[0], &key)
	if err != nil {
		return nil, err
	}
	val, _ := c.db.Get(key, nil)
	return resp.ConvertTo(len(val))
}
