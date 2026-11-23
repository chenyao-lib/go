package srv

import (
	"encoding/json"
	"fmt"

	"github.com/chenyao-lib/go/payloadcodec"
)

func (c *Client) marshalRPCMessage(msg RPCMessage) ([]byte, error) {
	if c.codec != nil {
		plaintext := []byte(msg.Data)
		if len(plaintext) == 0 {
			plaintext = []byte("null")
		}

		payload, err := c.codec.Encode(c.payloadMeta(msg, payloadcodec.ServerToClient), plaintext)
		if err != nil {
			return nil, fmt.Errorf("encode rpc payload: %w", err)
		}
		msg.Data = payload
	}

	return json.Marshal(msg)
}

func (c *Client) decodeRPCMessage(msg *RPCMessage, readLimit int64) error {
	if c.codec == nil {
		return nil
	}

	plaintext, err := c.codec.Decode(c.payloadMeta(*msg, payloadcodec.ClientToServer), msg.Data)
	if err != nil {
		return fmt.Errorf("decode rpc payload: %w", err)
	}
	messageSize := int64(len(plaintext)) + int64(len(msg.Method)) + int64(len(msg.Session)) + int64(len(msg.Error))
	if messageSize > readLimit {
		return fmt.Errorf("decoded RPC message exceeds %d bytes", readLimit)
	}
	msg.Data = append(json.RawMessage(nil), plaintext...)
	return nil
}

func (c *Client) payloadMeta(msg RPCMessage, direction payloadcodec.Direction) payloadcodec.Meta {
	return payloadcodec.Meta{
		ClientID:  c.ID,
		Session:   msg.Session,
		Method:    msg.Method,
		Error:     msg.Error,
		Direction: direction,
	}
}
