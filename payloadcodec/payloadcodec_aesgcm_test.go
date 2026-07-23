package payloadcodec

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestAESGCMCodecRoundTrip(t *testing.T) {
	codec := newTestCodec(t)
	meta := Meta{
		ClientID:  "machine-1",
		Session:   "session-1",
		Method:    "pay",
		Direction: ClientToServer,
	}
	plaintext := []byte("{\"amount\":100}")

	payload, err := codec.Encode(meta, plaintext)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	got, err := codec.Decode(meta, payload)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("Decode() = %s, want %s", got, plaintext)
	}
}

func TestAESGCMCodecRejectsMetadataTampering(t *testing.T) {
	codec := newTestCodec(t)
	meta := Meta{
		ClientID:  "machine-1",
		Session:   "session-1",
		Method:    "pay",
		Direction: ClientToServer,
	}
	payload, err := codec.Encode(meta, []byte("{\"amount\":100}"))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	meta.Method = "refund"
	if _, err := codec.Decode(meta, payload); err == nil {
		t.Fatal("Decode() accepted modified metadata")
	}
}

func TestAESGCMCodecRejectsCiphertextTampering(t *testing.T) {
	codec := newTestCodec(t)
	meta := Meta{
		ClientID:  "machine-1",
		Session:   "session-1",
		Method:    "pay",
		Direction: ClientToServer,
	}
	payload, err := codec.Encode(meta, []byte("{\"amount\":100}"))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	var envelope encryptedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		t.Fatalf("DecodeString() error = %v", err)
	}
	ciphertext[len(ciphertext)-1] ^= 0xff
	envelope.Ciphertext = base64.StdEncoding.EncodeToString(ciphertext)
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	if _, err := codec.Decode(meta, tampered); err == nil {
		t.Fatal("Decode() accepted modified ciphertext")
	}
}

func TestNewFactoryRejectsUnsupportedAlgorithm(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	if _, err := NewFactory("unknown", key, "test"); err == nil {
		t.Fatal("NewFactory() accepted unsupported algorithm")
	}
}

func newTestCodec(t *testing.T) *AESGCMCodec {
	t.Helper()
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	codec, err := NewAESGCMCodec(key, "test-key")
	if err != nil {
		t.Fatalf("NewAESGCMCodec() error = %v", err)
	}
	return codec
}
