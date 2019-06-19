package leveldb

import (
	"strconv"

	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/wzshiming/resp"
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

func toInteger(r resp.Reply) (int64, error) {
	b := toBytes(r)
	if b == nil {
		return 0, nil
	}
	i, err := strconv.ParseInt(string(b), 0, 0)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func cloneBytes(data []byte) []byte {
	buf := make([]byte, len(data))
	copy(buf, data)
	return buf
}

func getBit(i int) byte {
	switch i {
	default:
		return 0
	case 0:
		return 1 << 7
	case 1:
		return 1 << 6
	case 2:
		return 1 << 5
	case 3:
		return 1 << 4
	case 4:
		return 1 << 3
	case 5:
		return 1 << 2
	case 6:
		return 1 << 1
	case 7:
		return 1 << 0
	}
}
