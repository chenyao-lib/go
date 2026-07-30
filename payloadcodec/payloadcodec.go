// Package payloadcodec 定义双向 RPC 载荷编解码接口，并提供绑定消息元数据的
// AES-GCM 实现。clt 和 srv 使用它只转换 RPCMessage.Data，不改变外层 JSON
// 协议和 Session、Method、Error 等字段。
//
// # AES-GCM
//
//	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
//	codec, err := payloadcodec.NewAESGCMCodec(key, "key-2026-01")
//	if err != nil {
//		return err
//	}
//
// 客户端应在连接前安装 Codec：
//
//	client := clt.NewClient(ctx, addr, "worker", "worker-01")
//	if err := client.SetPayloadCodec(codec); err != nil {
//		return err
//	}
//
// 服务端通常在 Authenticator 中根据客户端身份创建 Codec：
//
//	server.SetAuthenticator(func(
//		ctx context.Context,
//		client *srv.Client,
//		data []byte,
//	) (*srv.AuthResult, error) {
//		codec, err := payloadcodec.NewAESGCMCodec(key, "key-2026-01")
//		if err != nil {
//			return nil, err
//		}
//		return &srv.AuthResult{
//			Type:  "worker",
//			ID:    "worker-01",
//			Codec: codec,
//		}, nil
//	})
//
// 通信两端必须使用相同的 AES 密钥和 key ID。AESGCMCodec 会把 ClientID、
// Session、Method、Error、Direction 和算法信息作为附加认证数据；任一字段
// 不一致都会导致 Decode 返回认证失败，而不是产生未经验证的明文。
//
// # 自定义实现
//
// 自定义 Codec 应保证 Encode 返回可直接放进 JSON data 字段的 json.RawMessage，
// Decode 返回原始业务 JSON。Factory 可按 clientType/clientID 为每个连接选择
// 不同密钥或算法。实现应把 Meta 纳入完整性校验，并限制输入大小，避免在解码
// 未可信载荷时无限分配内存。
//
// 内置 AESGCMCodec 限制明文最大 1 MiB、加密载荷最大 2 MiB，并为每条消息生成
// 独立随机 nonce。
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
