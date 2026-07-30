package payloadcodec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/chenyao-lib/go/log"
)

const (
	AESGCMV1           = "aes-gcm-v1"
	envelopeVersion    = 1
	maxPlaintextSize   = 1 << 20
	maxWirePayloadSize = 2 << 20
)

type encryptedEnvelope struct {
	Version    int    `json:"v"`
	Algorithm  string `json:"alg"`
	KeyID      string `json:"kid,omitempty"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

type authenticatedMeta struct {
	Version   int       `json:"v"`
	Algorithm string    `json:"alg"`
	KeyID     string    `json:"kid,omitempty"`
	ClientID  string    `json:"client_id"`
	Session   string    `json:"session"`
	Method    string    `json:"method"`
	Error     string    `json:"error"`
	Direction Direction `json:"direction"`
}

type AESGCMCodec struct {
	key   []byte
	keyID string
}

func NewAESGCMCodec(base64Key, keyID string) (*AESGCMCodec, error) {
	keyText := strings.TrimSpace(base64Key)
	if keyText == "" {
		err := errors.New("client payload key is empty")
		log.Error("AES-GCM 初始化失败: key_id=%s, err=%v", keyID, err)
		return nil, err
	}

	key, err := base64.StdEncoding.DecodeString(keyText)
	if err != nil {
		log.Error("AES-GCM 密钥解码失败: key_id=%s, err=%v", keyID, err)
		return nil, fmt.Errorf("decode client payload key: %w", err)
	}
	if _, err := aes.NewCipher(key); err != nil {
		log.Error("AES-GCM 密钥无效: key_id=%s, err=%v", keyID, err)
		return nil, fmt.Errorf("invalid client payload key: %w", err)
	}

	codec := &AESGCMCodec{
		key:   append([]byte(nil), key...),
		keyID: strings.TrimSpace(keyID),
	}
	log.Info("AES-GCM 初始化成功: key_id=%s", codec.keyID)
	return codec, nil
}

func (c *AESGCMCodec) Encode(meta Meta, plaintext []byte) (json.RawMessage, error) {
	if len(plaintext) > maxPlaintextSize {
		return nil, fmt.Errorf("plaintext exceeds %d bytes", maxPlaintextSize)
	}

	aead, err := c.aead()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	aad, err := c.additionalData(meta)
	if err != nil {
		return nil, err
	}

	envelope := encryptedEnvelope{
		Version:    envelopeVersion,
		Algorithm:  AESGCMV1,
		KeyID:      c.keyID,
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(aead.Seal(nil, nonce, plaintext, aad)),
	}
	return json.Marshal(envelope)
}

func (c *AESGCMCodec) Decode(meta Meta, payload json.RawMessage) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("encrypted payload is empty")
	}
	if len(payload) > maxWirePayloadSize {
		return nil, fmt.Errorf("encrypted payload exceeds %d bytes", maxWirePayloadSize)
	}

	var envelope encryptedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("parse encrypted payload: %w", err)
	}
	if envelope.Version != envelopeVersion {
		return nil, fmt.Errorf("unsupported payload version %d", envelope.Version)
	}
	if envelope.Algorithm != AESGCMV1 {
		return nil, fmt.Errorf("unexpected payload algorithm %q", envelope.Algorithm)
	}
	if envelope.KeyID != c.keyID {
		return nil, fmt.Errorf("unexpected payload key id %q", envelope.KeyID)
	}

	aead, err := c.aead()
	if err != nil {
		return nil, err
	}
	nonce, err := base64.StdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode nonce: %w", err)
	}
	if len(nonce) != aead.NonceSize() {
		return nil, fmt.Errorf("invalid nonce length %d", len(nonce))
	}
	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}
	aad, err := c.additionalData(meta)
	if err != nil {
		return nil, err
	}

	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, errors.New("payload authentication failed")
	}
	if len(plaintext) > maxPlaintextSize {
		return nil, fmt.Errorf("plaintext exceeds %d bytes", maxPlaintextSize)
	}
	return plaintext, nil
}

func (c *AESGCMCodec) aead() (cipher.AEAD, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM: %w", err)
	}
	return aead, nil
}

func (c *AESGCMCodec) additionalData(meta Meta) ([]byte, error) {
	b, err := json.Marshal(authenticatedMeta{
		Version:   envelopeVersion,
		Algorithm: AESGCMV1,
		KeyID:     c.keyID,
		ClientID:  meta.ClientID,
		Session:   meta.Session,
		Method:    meta.Method,
		Error:     meta.Error,
		Direction: meta.Direction,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal authenticated metadata: %w", err)
	}
	return b, nil
}
