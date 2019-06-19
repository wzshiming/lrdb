package lrdb

import (
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
