package payloadcodec

import "encoding/json"

type Direction string

const (
	ClientToServer Direction = "c2s"
	ServerToClient Direction = "s2c"
)

type Meta struct {
	ClientID  string
	Session   string
	Method    string
	Error     string
	Direction Direction
}

type Codec interface {
	Encode(meta Meta, plaintext []byte) (json.RawMessage, error)
	Decode(meta Meta, payload json.RawMessage) ([]byte, error)
}

type Factory func(clientType, clientID string) (Codec, error)
