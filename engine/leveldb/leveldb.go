package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/wzshiming/lrdb/engine"
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

	commands.AddCommand("getbit", c.getbit)
	commands.AddCommand("setbit", c.setbit)
	commands.AddCommand("bitcount", c.bitcount)

	commands.AddCommand("append", c.append)
	commands.AddCommand("strlen", c.strlen)

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
