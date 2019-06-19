package client

import (
	"net"

	"github.com/wzshiming/resp"
)

// Connect It's a client connection.
type Connect struct {
	decoder *resp.Decoder
	encoder *resp.Encoder
}

// NewConnect Create a new connect.
func NewConnect(address string) (*Connect, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	return &Connect{
		decoder: resp.NewDecoder(conn),
		encoder: resp.NewEncoder(conn),
	}, nil
}

func (c *Connect) Send(r resp.Reply) error {
	return c.encoder.Encode(r)
}

func (c *Connect) Recv() (resp.Reply, error) {
	return c.decoder.Decode()
}

func (c *Connect) Cmd(r resp.Reply) (resp.Reply, error) {
	err := c.Send(r)
	if err != nil {
		return nil, err
	}
	return c.Recv()
}

func (c *Connect) CmdSubscribe(r resp.Reply, fun func(reply resp.Reply) error) error {
	err := c.Send(r)
	if err != nil {
		return err
	}

	for {
		reply, err := c.Recv()
		if err != nil {
			return err
		}
		err = fun(reply)
		if err != nil {
			return err
		}
	}
	return nil
}
