package clt

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/chenyao-lib/go/payloadcodec"
)

func (c *WSClient) SetPayloadCodec(codec payloadcodec.Codec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isReady {
		return errors.New("payload codec cannot be changed while connected")
	}
	c.codec = codec
	return nil
}

func (c *WSClient) marshalRPCMessage(msg RPCMessage) ([]byte, error) {
	codec := c.payloadCodec()
	if codec != nil {
		plaintext := []byte(msg.Data)
		if len(plaintext) == 0 {
			plaintext = []byte("null")
		}

		payload, err := codec.Encode(c.payloadMeta(msg, payloadcodec.ClientToServer), plaintext)
		if err != nil {
			return nil, fmt.Errorf("encode rpc payload: %w", err)
		}
		msg.Data = payload
	}

	return json.Marshal(msg)
}

func (c *WSClient) decodeRPCMessage(msg *RPCMessage) error {
	codec := c.payloadCodec()
	if codec == nil {
		return nil
	}

	plaintext, err := codec.Decode(c.payloadMeta(*msg, payloadcodec.ServerToClient), msg.Data)
	if err != nil {
		return fmt.Errorf("decode rpc payload: %w", err)
	}
	msg.Data = append(json.RawMessage(nil), plaintext...)
	return nil
}

func (c *WSClient) payloadCodec() payloadcodec.Codec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.codec
}

func (c *WSClient) payloadMeta(msg RPCMessage, direction payloadcodec.Direction) payloadcodec.Meta {
	return payloadcodec.Meta{
		ClientID:  c.ClientId,
		Session:   msg.Session,
		Method:    msg.Method,
		Error:     msg.Error,
		Direction: direction,
	}
}
