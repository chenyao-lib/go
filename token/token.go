// Package token 提供 "base64(payload).base64(hmac-sha256(payload))" 形式的
// 签名令牌，适用于平台/服务间签发与校验登录凭证，以及对 HTTP 请求体/响应体的
// HMAC 签名(X-Sign 头模式)。
//
// 高层 API：
//
//	tok := token.Issue(&token.LoginToken{
//		UserID: "u100", GameAddr: "192.168.1.10:54337",
//	}, 24*time.Hour, secret)
//
//	t, err := token.Parse(tok, secret) // 验签 + 解析 + 过期校验
//
// 原语(可承载任意自定义载荷)：
//
//	sig := token.Sign(body, secret)          // HMAC-SHA256
//	ok := token.Verify(body, sig, secret)    // 恒时比较
//	tok := token.EncodePayload(body, secret) // base64(payload).base64(sig)
//	body, err := token.Decode(tok, secret)   // 验签并还原载荷
//	body, err := token.DecodePayload(tok)    // 不验签拆载荷(仅供路由参考等非安全场景)
//
// LoginToken 的字段名(json tag)是各业务系统约定的标准载荷结构，勿随意改动。
//
// Issue、Decode 和 Parse 在 secret 为空时直接失败，避免用空密钥签发可伪造的
// 令牌；Issue 只修改副本，不会改写调用方传入的 LoginToken。
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// LoginToken 登录令牌载荷(各业务系统共用的标准字段)。
// 同一玩家的 token 由平台经 fishcenter 签发，game_addr 指向哈希环选定的 fishserver，
// gate 依据它做首帧路由，fishserver 校验其等于本机地址。
type LoginToken struct {
	UserID          string `json:"userid"`
	Name            string `json:"name"`
	NickName        string `json:"nickname"`
	AgentID         string `json:"agent_id"`
	LoginSuccessURL string `json:"login_success_url"`
	GameAddr        string `json:"game_addr"`
	Expire          int64  `json:"expire"`
}

// Sign HMAC-SHA256 签名
func Sign(data []byte, secret string) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(data)
	return h.Sum(nil)
}

// Verify 恒时时间比较校验签名
func Verify(data, signature []byte, secret string) bool {
	return hmac.Equal(Sign(data, secret), signature)
}

// Base64Encode StdEncoding 编码
func Base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// Base64Decode StdEncoding 解码
func Base64Decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

// MustBase64Decode 解码失败时返回 nil(不 panic)
func MustBase64Decode(s string) []byte {
	data, _ := base64.StdEncoding.DecodeString(s)
	return data
}

// EncodePayload 载荷编码为令牌: base64Std(payload) + "." + base64Std(hmac)
func EncodePayload(payload []byte, secret string) string {
	return Base64Encode(payload) + "." + Base64Encode(Sign(payload, secret))
}

// DecodePayload 无验签拆出载荷字节。
// 仅用于路由参考等非安全场景(如 gate 提取 game_addr)；安全校验必须用 Decode/Parse。
func DecodePayload(tokenStr string) ([]byte, error) {
	parts := strings.SplitN(tokenStr, ".", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid token format")
	}
	return Base64Decode(parts[0])
}

// Decode 验签并返回载荷字节
func Decode(tokenStr, secret string) ([]byte, error) {
	if secret == "" {
		return nil, errors.New("token secret is empty")
	}
	parts := strings.SplitN(tokenStr, ".", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid token format")
	}
	payload, err := Base64Decode(parts[0])
	if err != nil {
		return nil, fmt.Errorf("payload base64 decode: %w", err)
	}
	sign, err := Base64Decode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("signature base64 decode: %w", err)
	}
	if !Verify(payload, sign, secret) {
		return nil, errors.New("invalid token signature")
	}
	return payload, nil
}

// Issue 填充过期时间(Expire)并签发令牌。
// 只修改副本，不会改写调用方传入的 LoginToken；t 为 nil、secret 为空或
// 序列化失败时返回空串。
func Issue(t *LoginToken, ttl time.Duration, secret string) string {
	if t == nil || secret == "" {
		return ""
	}
	issued := *t
	issued.Expire = time.Now().Add(ttl).Unix()
	data, err := json.Marshal(&issued)
	if err != nil {
		return ""
	}
	return EncodePayload(data, secret)
}

// DefaultClockSkew 是 Parse 默认容忍的时钟偏差。
//
// FIX 2026-11-09 跨机多开: Issue 记的是签发机时间，Parse 比的是验证机时间。
// 两台机器时钟差几秒，就会出现「token 刚过期」在一侧被拒、另一侧仍有效，
// 现场只表现为零星的 token expired，很难指向时钟。放开一个容差后行为对
// NTP 抖动不敏感；代价是有效期实际为 ttl+skew（默认 30s，对 24h TTL 无实质影响）。
var DefaultClockSkew = 30 * time.Second

// Parse 验签、解析载荷并校验过期时间（容忍 DefaultClockSkew 的时钟偏差）
func Parse(tokenStr, secret string) (*LoginToken, error) {
	return ParseWithSkew(tokenStr, secret, DefaultClockSkew)
}

// ParseWithSkew 同 Parse，由调用方指定可接受的时钟偏差；skew <= 0 表示精确校验。
func ParseWithSkew(tokenStr, secret string, skew time.Duration) (*LoginToken, error) {
	payload, err := Decode(tokenStr, secret)
	if err != nil {
		return nil, err
	}
	var t LoginToken
	if err := json.Unmarshal(payload, &t); err != nil {
		return nil, fmt.Errorf("payload json unmarshal: %w", err)
	}
	var grace int64
	if skew > 0 {
		grace = int64(skew / time.Second)
	}
	if time.Now().Unix() > t.Expire+grace {
		return nil, errors.New("token expired")
	}
	return &t, nil
}
