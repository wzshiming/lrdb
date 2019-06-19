package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/wzshiming/resp"
)

func (c *LevelDB) info(name string, args []resp.Reply) (resp.Reply, error) {
	stats := &leveldb.DBStats{}
	err := c.db.Stats(stats)
	if err != nil {
		return nil, err
	}
	return resp.ConvertTo(stats)
}
