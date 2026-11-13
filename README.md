# chenyao-lib/go

内部基础库。提供 WebSocket 双向 RPC（服务端/客户端）、进程内串行服务、etcd 服务注册与发现、
日志、配置、签名令牌、节点地址、HTTP API 客户端、Redis 连接租约和 RPC 载荷加密。

要求 Go 1.26 及以上。

## 安装

```bash
go get github.com/chenyao-lib/go@v1.3.7
```

版本以 tag 为准，消费方在 go.mod 里固定版本，不要把 `replace` 指向本地工作区。

## 包一览

| 包 | 用途 |
| --- | --- |
| `log` | 进程级日志：按天/大小滚动、备份清理、运行时级别、控制台开关、`_latest.log` 入口 |
| `config` | 泛型配置加载、文件查找、热更新、字段变更回调、进程级配置存取 |
| `token` | `base64(payload).base64(hmac-sha256(payload))` 签名令牌及 HMAC 原语 |
| `nodeaddr` | 服务节点地址（host:port）的唯一构造与解析入口 |
| `srv` | 带认证、客户端分组、双向 RPC、优雅关闭的 WebSocket 服务端 |
| `clt` | 带认证、自动重连、请求响应关联、服务端推送处理的 WebSocket 客户端 |
| `svc` | 进程内按名称注册的串行消息服务，支持超时、优雅停止和指标 |
| `srvsvcbinding` | 把每个 `srv.Client` 连接绑定到一个独立的 `svc.Instance`，含中间件生命周期 |
| `payloadcodec` | 双向 RPC 载荷编解码接口，内置 AES-GCM（只加密 `RPCMessage.Data`） |
| `etcd/register` | 基于 etcd 租约的服务注册、保活、自动重注册 |
| `etcd/watcher` | 从 etcd 前缀加载并监听节点，支持轮询/一致性哈希/自定义选点 |
| `redis/connlease` | 用 Redis 租约 + Pub/Sub + fencing epoch 保证同一会话只有一个有效连接 |
| `redis/leader` | 基于 Redis 租约 + Lua 的主节点选举，Go 与 Node.js 共用脚本和 key 布局 |
| `httpapi` | 约定式 HTTP API 客户端：JSON 数据信封、表单、可插拔加解密 |

## 各包速览

### log

```go
log.Init("gateway", "./logs", 100*1024*1024, log.LevelInfo, 10, 7*24*time.Hour)
defer log.Close()

log.Info("server started at %s", ":8080")
log.InfoM(map[string]any{"port": 8080}, "server started")
```

- 零配置可用：不调用 `Init` 时默认写 `./logs/log_YYYYMMDD.log`，级别 DEBUG。
- `Init` 之前的日志先缓冲（默认 1 秒，`LOG_INIT_GRACE_MS` 可覆盖，0 关闭），用于避免启动期出现两个日志文件。
- 文件 `<prefix>_YYYYMMDD.log`，超过 maxSize 转 `_N` 备份；`maxBackups`/`maxAge` 控制清理。
- 目录里维护 `<prefix>_latest.log` 硬链接指向当前文件，跟踪用 `tail -F`；异常退出遗留的 `.tmp` 会在下次滚动时清理。
- 运行时接口：`SetLevel`、`SetConsole`、`SetTimezone`、`SetRetention`、`SetBufferConfig`。
- 不要直接 `os.Exit`：用 `Fatal`/`FatalM` 才会先刷新队列。

注意：`SetConsole(false)` 只影响 `main` 之后的输出。业务包在 `init()` 里的日志早于 main，
仍会按默认配置（控制台开、DEBUG）打印；部署侧如果需要彻底关闭控制台，应重定向 stdout。

### config

解析格式由调用方注入，不绑定 JSON/YAML/TOML：

```go
decoder := config.DecoderFunc(func(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
})

cfg := config.Init[AppConfig]("./app.json", decoder)
other := config.Get[AppConfig]() // 其他包取同一份快照
```

需要控制查找方式、监听或防抖时用 `Loader`：

```go
loader := config.MustNewLoader[AppConfig](config.Options{
	AppName:    "gateway",
	Extensions: []string{".json"},
	Decoder:    decoder,
	Watch:      true,
	Debounce:   500 * time.Millisecond,
})
loader.OnChange("server.addr", func(oldVal, newVal any) { /* ... */ })
cfg := loader.MustLoad()
```

`FilePath`（精确路径）、`FileName`（exe 目录→工作目录）、`AppName+Extensions` 三选一。
热更新会替换内部指针，长驻代码用 `loader.Get()` 取最新快照，不要缓存首次 Load 的指针。

### token

```go
tok := token.Issue(&token.LoginToken{UserID: "u100", GameAddr: "192.168.1.10:54337"}, 24*time.Hour, secret)
t, err := token.Parse(tok, secret) // 验签 + 解析 + 过期校验

sig := token.Sign(body, secret)   // HMAC-SHA256
ok := token.Verify(body, sig, secret)
```

`secret` 为空时签发/校验直接失败。`DecodePayload` 不验签，只能用于路由参考等非安全场景。

### nodeaddr

服务发现、token 签发、WebSocket 拨号都用 `host:port` 字符串，本包保证同一地址只有一种表示，
跨进程边界统一 `String()` 出、`ParseNodeAddr()` 入（处理 IPv6、空白、非法端口）。

```go
addr := nodeaddr.NodeAddr{Host: "10.0.0.8", Port: 8080}
s := addr.String()
back, err := nodeaddr.ParseNodeAddr(s)
```

### srv

```go
server := srv.NewServer(":8080")
server.SetOriginPatterns("https://example.com") // 默认允许所有 Origin，对外服务必须设置
server.RegisterHandler("worker", "job.run", func(c *srv.Client, data json.RawMessage) (any, error) {
	return RunResponse{Accepted: true}, nil
})
go func() { _ = server.Start() }()

// 退出时
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = server.Shutdown(ctx)
```

- 固定 `/ws` 路径；`SetAuthenticator` 校验令牌并返回规范化 `AuthResult{Type, ID, Codec}`。
- 服务端反向调用/推送：`GetClientByID`、`WalkClient`、`SendByID`、`Client.Call/CallT`。
- 生命周期：`SetOnClientReady`、`SetOnClientClose`（用 `srvsvcbinding` 时不要覆盖）。

### clt

```go
client := clt.NewClient(ctx, "127.0.0.1:8080", "worker", "worker-01")
client.SetDefaultCallTimeout(5 * time.Second)
client.RegisterHandler("job.cancel", func(c *clt.WSClient, data json.RawMessage) (any, error) {
	return CancelResponse{OK: true}, nil
})
go client.Start(func(c *clt.WSClient) { log.Info("connected: %s", c.Addr) })
defer client.Close()

raw, err := client.Call("job.run", RunRequest{ID: "42"})
result, raw, _, err := clt.CallT[RunResponse](client, "job.run", RunRequest{ID: "42"})
```

- host 不含协议时自动拼 `ws://host/ws`；完整 `ws://`/`wss://` 原样使用。
- 断线指数退避重连；认证被明确拒绝时 `Start` 返回并永久停止。
- `Send` 不等待响应；`Call/Send` 只在 `IsConnected`/`IsReady` 为 true 时使用。
- `Close` 后该实例不能重新 `Start`。

### svc

```go
type EchoDefinition struct{}

func (EchoDefinition) Name() string             { return "echo" }
func (EchoDefinition) Init(ctx context.Context) error { return nil }
func (EchoDefinition) Handlers() map[string]svc.Handler {
	return map[string]svc.Handler{
		"upper": func(ctx context.Context, args any) (any, error) {
			return strings.ToUpper(args.(string)), nil
		},
	}
}

echo := svc.NewService(EchoDefinition{})
_ = echo.Start()
defer echo.Stop()

result, err := echo.RawCall(ctx, "upper", "hello")   // 外部 goroutine
result, err = current.Call(ctx, "echo", "upper", "hello") // Handler 内调用其他服务
```

- 每个 Service 独立队列、串行执行；进程内名称唯一。
- `RawCall/RawSend` 不允许在 Handler 内调用，Handler 内用 `Call/Send`。
- 队列满、超时、服务停止分别用 `errors.Is` 判断 `ErrQueueFull`、`ErrCallTimeout` 等。
- `Stop` 先摘注册表，再在 DrainTimeout 内处理剩余消息；`Snapshot` 返回队列与超时指标。

### srvsvcbinding

```go
binding := srvsvcbinding.NewClientServiceBinding(
	server,
	func(client *srv.Client) svc.Instance {
		return svc.NewService(NewConnectionDefinition(client))
	},
	srvsvcbinding.WithMiddleware(authzMiddleware),
)
_ = server.Start()
```

连接 ready 后依次执行中间件、创建并启动 Service；带 Session 的消息走 `Instance.RawCall`，
无 Session 的走 `Instance.RawSend`。它会接管 `OnClientReady`、`OnClientClose` 和默认 Handler，
创建后不要再覆盖；额外生命周期逻辑用 Middleware。

### payloadcodec

```go
codec, err := payloadcodec.NewAESGCMCodec(base64Key, "key-2026-01")
client.SetPayloadCodec(codec) // 客户端：连接前安装

// 服务端：认证时按身份下发
return &srv.AuthResult{Type: "worker", ID: "worker-01", Codec: codec}, nil
```

只加密 `RPCMessage.Data`，外层 JSON 协议不变。两端密钥和 keyID 必须一致；ClientID、Session、
Method、Error、Direction 都作为附加认证数据。明文上限 1 MiB，密文上限 2 MiB，每条消息独立 nonce。

### etcd/register

```go
reg, err := register.NewRegister(
	"127.0.0.1:2379,127.0.0.2:2379",
	register.RegisterOption{Key: "services/gateway/gw-01", Value: "10.0.0.8:8080", TTL: 10},
)
defer reg.Close()
if err := reg.Register(); err != nil { /* ... */ }
```

地址或实例 ID 会变时用 `KeyFunc`/`ValueFunc`。租约丢失默认自动重注册；要关闭用
`DisableAutoReRegister`（直接给 `AutoReRegister=false` 会被默认值覆盖）。`OnLeaseLost` 可观测丢失事件。

### etcd/watcher

```go
nodes, err := watcher.NewWatcher("127.0.0.1:2379", "services/game/", 10, watcher.WithHashRing(150))
_ = nodes.Start()
defer nodes.Close()

addr := nodes.GetNode(playerID)
```

默认轮询；`WithRoundRobin` 显式轮询，`WithHashRing(n)` 为每个真实节点建 n 个虚拟节点，
`WithStrategy` 安装自定义并发安全策略。必须先 `Start` 再 `GetNode`，退出时 `Close`。

### redis/connlease

```go
manager, err := connlease.Open(ctx, connlease.Config{
	Addr:          "127.0.0.1:6379",
	ServerAddr:    "gateway-01",
	KeyPrefix:     "session:",
	SessionTTL:    30 * time.Second,
	RenewInterval: 10 * time.Second,
})
defer manager.Close()

handle, err := manager.Register(ctx, userID, func(reason string) { /* 被踢下线 */ })
// 每条消息处理前
if err := manager.Validate(handle); err != nil { /* ErrOwnershipLost */ }
// 底层连接关闭时
manager.Unregister(ctx, handle)
```

`ServerAddr` 必须唯一标识当前节点；续租间隔必须小于租约 TTL。所有权变化通过踢线回调通知。

### redis/leader

```go
election, err := leader.Open(ctx, leader.Config{
	Addr:     "127.0.0.1:6379", // host:port，单地址，不支持 Sentinel/Cluster
	Password: "",               // 没有密码留空
	DB:       0,                // 与业务读写同一个库
	Group:    "billing",        // 同一 group 的进程竞争同一个主节点
	Owner:    "billing-01",     // 必须唯一标识一个进程
	TTL:   10 * time.Second, // 默认 10 秒；续租/重试间隔默认 TTL/3
	OnChange: func(ev leader.Event) {
		if ev.IsLeader {
			startPrimaryWork()
		} else {
			stopPrimaryWork()
		}
	},
})
defer election.Close()

election.IsLeader() // 热路径：纯内存，并校验本地租约是否到期
election.Epoch()    // 当前任期 fencing token，非主节点为 0
election.Leader(ctx)
election.Resign(ctx) // 主动让位并停止选举
```

Redis 短暂不可用时本地租约到期前继续担任主节点，到期自动降级；被其它节点接管立即降级。
关闭时尽力释放租约，其它节点不用等 TTL。Node.js 等其它语言按 `leader.go` 包注释里的
key 布局（`leader:{group}:owner` / `leader:{group}:epoch`）执行同样的两个 Lua 脚本
（`acquireScript` / `releaseScript`）即可参与同一个 group 的选举。

### httpapi

```go
client := httpapi.NewHttpClient(5, nil) // timeoutSecs 超出 1..120 时按 5 秒
var result CreateResponse
err := client.Call("https://api.example.com/create", CreateRequest{Name: "demo"}, "zh-CN", &result)

var apiErr *httpapi.APIError
if errors.As(err, &apiErr) { /* code != 0 的业务错误 */ }
```

- `Call` 把业务请求放进 `data`，附 `lang` 和纳秒时间戳；`out` 必须是指针。
- 加密客户端：`NewAesCipherClient(5, base64Key)`（AES-CBC + PKCS#7）。
- AES-CBC 不校验完整性，旧协议可用 `HttpClient.VerifyResponse` 校验 `data/timestamp/signature`。
- 表单：`CallFormMap`（`__input` 字段）、`RequestForm`（multipart）。

## 典型组合

```
服务端：srv + srvsvcbinding + svc + payloadcodec(+ log/config/token)
客户端：clt + payloadcodec
发现：  etcd/register（注册） + etcd/watcher（选点）
会话：  redis/connlease（同一 session 全局单连接）
选主：  redis/leader（多个同构服务只保留一个主节点）
```

## 约定

- 发布：提交后打 tag（`vX.Y.Z`），消费方按 tag 升级；共享库的 commit/tag/push 由维护者执行。
- 修改公共 API 时同步更新本文件对应段落。
