package httpapi

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

// AESCipher implements Encryptor with AES-CBC and PKCS#7 padding. Ciphertext is
// encoded as base64 so it can be safely transported in a JSON or form field.
type AESCipher struct {
	base64Key string
}

func NewAESCipher(base64Key string) *AESCipher {
	return &AESCipher{base64Key: strings.TrimSpace(base64Key)}
}

// NewEncryptor is kept for compatibility. New code should use NewAESCipher.
func NewEncryptor(base64Key string) *AESCipher {
	return NewAESCipher(base64Key)
}

func (e *AESCipher) Encrypt(plaintext []byte) (string, error) {
	key, err := e.key()
	if err != nil {
		return "", err
	}
	if len(plaintext) == 0 {
		return "", errors.New("empty plaintext")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	plaintext = pkcs7Pad(plaintext, block.BlockSize())
	ciphertext := make([]byte, len(plaintext))
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(append(iv, ciphertext...)), nil
}

func (e *AESCipher) Decrypt(base64CipherText string) ([]byte, error) {
	key, err := e.key()
	if err != nil {
		return nil, err
	}
	base64CipherText = strings.TrimSpace(base64CipherText)
	if base64CipherText == "" {
		return nil, errors.New("empty ciphertext")
	}
	cipherTextWithIV, err := base64.StdEncoding.DecodeString(base64CipherText)
	if err != nil {
		return nil, err
	}
	if len(cipherTextWithIV) <= aes.BlockSize || len(cipherTextWithIV[aes.BlockSize:])%aes.BlockSize != 0 {
		return nil, errors.New("invalid ciphertext length")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	iv := cipherTextWithIV[:aes.BlockSize]
	ciphertext := append([]byte(nil), cipherTextWithIV[aes.BlockSize:]...)
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(ciphertext, ciphertext)
	return pkcs7Unpad(ciphertext, block.BlockSize())
}

func (e *AESCipher) key() ([]byte, error) {
	if e == nil || e.base64Key == "" {
		return nil, errors.New("empty AES key")
	}
	key, err := base64.StdEncoding.DecodeString(e.base64Key)
	if err != nil {
		return nil, fmt.Errorf("decode AES key: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("invalid AES key length: %d", len(key))
	}
}

func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	return append(data, bytes.Repeat([]byte{byte(padding)}, padding)...)
}

func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty padded data")
	}
	padding := int(data[len(data)-1])
	if padding < 1 || padding > blockSize || padding > len(data) {
		return nil, errors.New("invalid padding")
	}
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, errors.New("invalid padding")
		}
	}
	return data[:len(data)-padding], nil
}
