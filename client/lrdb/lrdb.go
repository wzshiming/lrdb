package lrdb

import (
	"errors"
	"time"

	"github.com/wzshiming/lrdb/reply"
	"github.com/wzshiming/resp"
	"github.com/wzshiming/resp/client"
)

type Client struct {
	client.Connect
}

func NewClient(address string) (*Client, error) {
	conn, err := client.NewConnect(address)
	if err != nil {
		return nil, err
	}

	c := &Client{
		*conn,
	}

	_, err = c.Ping()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Execute execute a command
func (c *Client) Execute(in, out interface{}) (err error) {
	req, err := resp.ConvertTo(in)
	if err != nil {
		return err
	}
	res, err := c.Cmd(req)
	if err != nil {
		return err
	}
	if re, ok := res.(resp.ReplyError); ok {
		return errors.New(string(re))
	}
	if out != nil {
		err = resp.ConvertFrom(res, out)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close close the client
// Ask the server to close the connection.
// The connection is closed as soon as all pending replies have been written to the client.
func (c *Client) Close() error {
	return c.Execute([]string{"quit"}, nil)
}

// Ping Returns PONG if no argument is provided.
func (c *Client) Ping() (bool, error) {
	req, err := resp.ConvertTo([]string{"ping"})
	if err != nil {
		return false, err
	}
	res, err := c.Cmd(req)
	if err != nil {
		return false, err
	}
	return resp.Equal(res, reply.PONG), nil
}

// PingMessage return a copy of the argument as a bulk.
// This command is often used to test if a connection is still alive, or to measure latency.
func (c *Client) PingMessage(message string) (b bool, err error) {
	var r string
	return r == message, c.Execute([]string{"ping", message}, &r)
}

// Echo Returns message
func (c *Client) Echo(message string) (r string, err error) {
	return r, c.Execute([]string{"echo", message}, &r)
}

// Time Returns server current time
func (c *Client) Time() (t time.Time, err error) {
	var b [2]int64
	err = c.Execute([]string{"time"}, &b)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(b[0], b[1]), nil
}

// Info Returns server info
func (c *Client) Info() (info *Info, err error) {
	return info, c.Execute([]string{"info"}, &info)
}

// Set key to hold the string value.
// If key already holds a value, it is overwritten, regardless of its type.
func (c *Client) Set(k, v string) (err error) {
	return c.Execute([]string{"set", k, v}, nil)
}

// Rename Renames key to newkey.
// It returns an error when key does not exist.
// If newkey already exists it is overwritten, when this happens RENAME executes an implicit DEL operation,
// so if the deleted key contains a very big value it may cause high latency even
// if RENAME itself is usually a constant-time operation.
func (c *Client) Rename(k, nk string) (err error) {
	return c.Execute([]string{"rename", k, nk}, nil)
}

// Get the value of key.
// If the key does not exist the special value nil is returned.
func (c *Client) Get(k string) (r string, err error) {
	return r, c.Execute([]string{"get", k}, &r)
}

// GetSet Atomically sets key to value and returns the old value stored at key.
func (c *Client) GetSet(k, v string) (r string, err error) {
	return r, c.Execute([]string{"getset", k, v}, &r)
}

// Del Removes the specified keys. A key is ignored if it does not exist.
func (c *Client) Del(k ...string) (d int, err error) {
	return d, c.Execute(append([]string{"del"}, k...), &d)
}

// Exists Returns if key exists.
// The user should be aware that if the same existing key is mentioned in the arguments multiple times,
// it will be counted multiple times.
// So if somekey exists, EXISTS somekey somekey will return 2.
func (c *Client) Exists(k ...string) (d int, err error) {
	return d, c.Execute(append([]string{"exists"}, k...), &d)
}

// SetBit Sets or clears the bit at offset in the string value stored at key.
// The bit is either set or cleared depending on value.
// When key does not exist, a new string value is created.
// return original bit value stored at offset.
func (c *Client) SetBit(k string, off int, f bool) (b bool, err error) {
	return b, c.Execute([]interface{}{"setbit", off, b}, &b)
}

// GetBit Returns the bit value at offset in the string value stored at key.
func (c *Client) GetBit(k string, off int) (b bool, err error) {
	return b, c.Execute([]interface{}{"getbit", off}, &b)
}

// BitCount Returns the bit value at offset in the string value stored at key.
func (c *Client) BitCount(k string) (b int, err error) {
	return b, c.Execute([]interface{}{"bitcount", k}, &b)
}

// BitCountRange Like BitCount.
// It is possible to specify the counting operation only in an interval passing the additional arguments start and end.
func (c *Client) BitCountRange(k string, start int, end int) (b int, err error) {
	return b, c.Execute([]interface{}{"bitcount", k, start, end}, &b)
}

// Append If key already exists and is a string, this command appends the value at the end of the string.
// If key does not exist it is created and set as an empty string, so APPEND will be similar to SET in this special case.
func (c *Client) Append(k string, v string) (b int, err error) {
	return b, c.Execute([]string{"append", k, v}, &b)
}

// StrLen Returns the length of the string value stored at key.
func (c *Client) StrLen(k string) (b int, err error) {
	return b, c.Execute([]string{"strlen", k}, &b)
}

// Keys key-value pairs with keys in range (key_start, key_end]. ("", ""] means no range limit.
func (c *Client) Keys(start, end string, limit int) (b []string, err error) {
	return b, c.Execute([]interface{}{"keys", start, end, limit}, &b)
}

// RKeys Like keys, but in reverse order.
func (c *Client) RKeys(start, end string, limit int) (b []string, err error) {
	return b, c.Execute([]interface{}{"rkeys", start, end, limit}, &b)
}

// Scan key-value pairs with keys and values in range (key_start, key_end]. ("", ""] means no range limit.
func (c *Client) Scan(start, end string, limit int) (m map[string]string, err error) {
	return m, c.Execute([]interface{}{"scan", start, end, limit}, &m)
}

// RScan key-value pairs with keys and values in range (key_start, key_end]. ("", ""] means no range limit.
func (c *Client) RScan(start, end string, limit int) (m map[string]string, err error) {
	return m, c.Execute([]interface{}{"rscan", start, end, limit}, &m)
}
