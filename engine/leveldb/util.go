package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/util"
)

// bytesPrefix returns key range that satisfy the given prefix.
func bytesPrefix(prefix []byte) *util.Range {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return &util.Range{prefix, limit}
}

// bytesNext returns the next in the current bytes.
func bytesNext(data []byte) []byte {
	for i := len(data) - 1; i >= 0; i-- {
		c := data[i]
		if c < 0xff {
			limit := make([]byte, len(data))
			copy(limit, data)
			limit[i] = c + 1
			return limit
		}
	}
	return nil
}

func cloneBytes(data []byte) []byte {
	buf := make([]byte, len(data))
	copy(buf, data)
	return buf
}
