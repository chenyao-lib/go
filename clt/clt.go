// Package clt 实现带认证、自动重连、请求响应关联和服务端推送处理的
// WebSocket RPC 客户端。
//
// # 建立连接
//
//	func run(ctx context.Context) {
//		client := clt.NewClient(ctx, "127.0.0.1:8080", "worker", "worker-01")
//		client.SetDefaultCallTimeout(5 * time.Second)
//		client.SetCallTimeout("job.run", 30*time.Second)
//		client.SetHandlerQueueSize(512)
//
//		client.RegisterHandler("job.cancel", func(
//			c *clt.WSClient,
//			data json.RawMessage,
//		) (any, error) {
//			var req CancelRequest
//			if err := json.Unmarshal(data, &req); err != nil {
//				return nil, err
//			}
//			return CancelResponse{OK: true}, nil
//		})
//
//		go client.Start(func(c *clt.WSClient) {
//			log.Info("connected: %s", c.Addr)
//		})
//		defer client.Close()
//	}
//
// host 不含协议时会自动组成 ws://host/ws；传入完整 ws:// 或 wss:// 地址时
// 原样使用。Start 会持续运行并在断线后指数退避重连，直到父 context 取消或
// Close 被调用，因此通常放在独立 goroutine 中。Close 会永久停止该实例，
// 关闭后不能重新 Start。
//
// # 调用与推送
//
// Call 发送带 Session 的同步 RPC，并返回 json.RawMessage：
//
//	raw, err := client.Call("job.run", RunRequest{ID: "42"})
//
// 已知响应类型时可使用 CallT：
//
//	result, raw, _, err := clt.CallT[RunResponse](
//		client, "job.run", RunRequest{ID: "42"},
//	)
//
// Send 发送不等待响应的消息。RegisterHandler 用于处理服务端主动发送的调用；
// 应在 Start 前注册。Call/Send 只应在 IsConnected 或 IsReady 为 true 时使用。
//
// # 认证和载荷加密
//
// 默认认证载荷包含 NewClient 传入的 clientType 和 clientId。需要附加认证字段时
// 在 Start 前调用 SetAuthPayload。调用 SetPayloadCodec 可加密 RPC 的 Data
// 字段；它只能在未连接状态设置，并且必须与服务端认证返回的 Codec 使用相同
// 密钥和 key ID。
//
// 服务端明确拒绝认证（AuthResponse.OK=false）时 Start 会返回并永久停止；
// 网络错误、读写超时、响应格式错误等临时故障会按指数退避继续重试。
//
// 心跳间隔、拨号超时、控制帧写超时、默认调用超时、单方法调用超时和处理队列
// 容量都应在 Start 前配置。SetReadDeadline 仅为兼容保留，coder/websocket 不
// 支持单连接读超时，调用不会生效。WaitStopSignal 是命令行程序等待
// SIGINT/SIGTERM 并清理客户端的便捷入口。
package clt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chenyao-lib/go/payloadcodec"

	"github.com/chenyao-lib/go/log"

	"github.com/coder/websocket"
	"github.com/outrigdev/goid"
)

type RPCMessage struct {
	Session string          `json:"session"` // 唯一会话 ID
	Method  string          `json:"method"`  // 调用的方法名
	Data    json.RawMessage `json:"data"`    // 请求参数或响应数据
	Error   string          `json:"error"`   // 错误信息
}

const defaultHandlerQueue = 256

const defaultHeartbeatInterval = 20 * time.Second

const defaultDialTimeout = 10 * time.Second

const defaultSendQueue = 100

// ErrAuthRejected 表示服务端明确拒绝了认证信息（AuthResponse.OK=false）。
// 该错误不可重试；网络错误、超时和响应格式错误不属于此错误，Start 会继续重试。
var ErrAuthRejected = errors.New("authentication rejected by server")

type AuthPayloadFunc func(c *WSClient) (any, error)

type WSClient struct {
	Addr         string
	ClientType   string
	ClientId     string
	Conn         *websocket.Conn
	IP           string
	Port         int
	SendCh       chan []byte // 当前连接的发送队列，每次重连更换
	Ctx          context.Context
	Cancel       context.CancelFunc
	mu           sync.RWMutex
	isReady      bool
	authConn     *websocket.Conn // dialed connection until authentication completes, guarded by mu
	connCtx      context.Context
	outbound     *outboundQueue
	codec        payloadcodec.Codec
	pumpsDone    map[chan struct{}]struct{} // current and draining connections, guarded by mu
	closeOnce    sync.Once
	closeDone    chan struct{}
	handlerPumps sync.Map // goroutine ID of each running handler pump

	pending     map[string]chan *RPCMessage
	pendingMu   sync.Mutex
	sessionSeed int64

	// handlers 存储 method 到 函数的映射
	handlers         map[string]func(clt *WSClient, data json.RawMessage) (any, error)
	handlersMu       sync.RWMutex
	HandlerCh        chan RPCMessage // 当前连接的服务端请求队列，每次重连更换
	handlerQueueSize atomic.Int64

	closed              atomic.Bool  // 标记是否已彻底关闭
	running             atomic.Bool  // 同一客户端仅允许运行一个连接循环
	readLimit           int64        // 读大小限制
	writeTimeout        atomic.Int64 // 写超时，默认5秒
	defaultCallTimeout  atomic.Int64 // Call 默认超时，默认5秒
	heartbeatInterval   atomic.Int64 // 心跳发送间隔，默认 20 秒
	dialTimeout         atomic.Int64 // 单次拨号超时，默认 10 秒
	readDeadline        atomic.Int64 // 读超时时间，默认 30 秒
	controlWriteTimeout atomic.Int64 // 控制帧写超时（Ping/Close），默认 2 秒
	authPayload         AuthPayloadFunc
	callTimeouts        map[string]time.Duration
	callTimeoutsMu      sync.RWMutex
}

type AuthMessage struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type AuthResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (c *WSClient) readPump(ctx context.Context, conn *websocket.Conn, handlerCh chan RPCMessage, sendCh chan []byte, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer func() {
		cancel()
		wg.Done()
	}()

	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			switch websocket.CloseStatus(err) {
			case websocket.StatusPolicyViolation:
				log.Warn("收到不可恢复的关闭码 (1008): %v，准备彻底停止客户端", err)
				c.cleanPendingWithError("authentication failed")
				c.Cancel()
				return
			case websocket.StatusGoingAway:
				log.Info("服务器主动关闭连接 (1001)，将自动重连: %v", err)
				return
			case websocket.StatusNoStatusRcvd:
				log.Info("收到关闭码 1005 (No Status Received)，将自动重连")
				return
			case websocket.StatusNormalClosure:
				return
			default:
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Error("读取错误: %v", err)
				return
			}
		}
		switch msgType {
		case websocket.MessageText:
			var msg RPCMessage
			jerr := json.Unmarshal(data, &msg)
			if jerr != nil {
				log.Info("收到非法消息: %s", string(data))
				continue
			}
			if err := c.decodeRPCMessage(&msg); err != nil {
				log.Warn("服务端消息解密失败: client=%s, method=%s, session=%s, err=%v",
					c.ClientId, msg.Method, msg.Session, err)
				continue
			}
			if msg.Method == "session.kicked" && msg.Session == "" {
				log.Warn("会话已被新连接替换，停止重连: client=%s", c.ClientId)
				c.closed.Store(true)
				c.cleanPendingWithError("session kicked")
				c.Cancel()
				return
			}
			if c.otherResponse(&msg) {
				continue
			}
			if msg.Method == "" {
				log.Error("没有处理方法: %s", string(data))
				continue
			}
			c.enqueueTextMessage(ctx, handlerCh, sendCh, msg)

		case websocket.MessageBinary:
			log.Info("收到二进制，大小为: %d 字节", len(data))
		}
	}
}

// outboundQueue 给内部 Call 附带期限，同时保留公开 SendCh 的字节队列和发送顺序。
type outboundQueue struct {
	frames chan []byte
	calls  sync.Map // 帧首字节的地址 -> context.Context
	cancel context.CancelFunc

	admissionMu sync.Mutex // 将 Send 入队与本次连接的断开串行化
}

func (q *outboundQueue) stop() {
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	q.cancel()
}

// canWrite 检查已出队的 RPC 是否仍可发送。普通帧没有对应的上下文。
func (q *outboundQueue) canWrite(frame []byte) bool {
	if len(frame) == 0 {
		return true
	}
	ctx, ok := q.calls.LoadAndDelete(&frame[0])
	return !ok || ctx.(context.Context).Err() == nil
}

func (c *WSClient) writePump(ctx context.Context, conn *websocket.Conn, queue *outboundQueue, wg *sync.WaitGroup) {
	ticker := time.NewTicker(c.heartbeatPeriod())
	defer func() {
		ticker.Stop()
		queue.stop()
		conn.CloseNow()
		wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			// 退出前优雅关闭协议连接
			_ = conn.Close(websocket.StatusNormalClosure, "")
			return
		case msg, ok := <-queue.frames:
			if !ok {
				return
			}
			if !queue.canWrite(msg) {
				continue
			}
			// 已开始写入的帧使用连接的写期限，避免 RPC 等待超时关闭连接。
			writeCtx, cancel := context.WithTimeout(ctx, time.Duration(c.writeTimeout.Load()))
			err := conn.Write(writeCtx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				log.Error("writePump: write message failed: %v", err)
				return
			}
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, time.Duration(c.controlWriteTimeout.Load()))
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				log.Error("writePump: ping failed: %v", err)
				return
			}
		}
	}
}

// sendQueue 返回当前连接的发送队列。队列随连接更换，断线期间排队的消息和
// 旧连接处理器迟到的响应不会被下一次连接消费。
func (c *WSClient) sendQueue() (*outboundQueue, context.Context) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.isReady || c.Conn == nil || c.outbound == nil || c.connCtx == nil {
		return nil, nil
	}
	return c.outbound, c.connCtx
}

func (c *WSClient) Send(method string, arg any) error {
	// 连接状态检查
	if c.closed.Load() {
		return errors.New("client already closed")
	}
	queue, connCtx := c.sendQueue()
	if queue == nil {
		return errors.New("connection is not ready, reconnecting")
	}

	dataBytes, err := json.Marshal(arg)
	if err != nil {
		return fmt.Errorf("json.Marshal arg failed: %w", err)
	}

	req := RPCMessage{
		Session: "",
		Method:  method,
		Data:    dataBytes, // 放入 RawMessage
	}

	b, err := c.marshalRPCMessage(req)
	if err != nil {
		return fmt.Errorf("marshal rpc message failed: %w", err)
	}

	queue.admissionMu.Lock()
	defer queue.admissionMu.Unlock()
	select {
	case <-connCtx.Done():
		return c.callDoneError(connCtx, "send request timeout")
	default:
	}

	var enqueueErr error
	select {
	case queue.frames <- b:
	default:
		enqueueErr = errors.New("send channel is full, message dropped")
	}
	// 父上下文也可以直接取消连接，在返回前再次检查。
	if connCtx.Err() != nil {
		return c.callDoneError(connCtx, "send request timeout")
	}
	return enqueueErr
}

func (c *WSClient) Call(method string, arg any) (json.RawMessage, error) {
	// 如果客户端已关闭或连接未就绪，直接返回错误
	if c.closed.Load() {
		return nil, errors.New("client already closed")
	}
	queue, connCtx := c.sendQueue()
	if queue == nil {
		return nil, errors.New("connection is not ready, reconnecting")
	}

	ctx, cancel := c.callContext(connCtx, method)
	defer cancel()

	// 序列化参数
	dataBytes, err := json.Marshal(arg)
	if err != nil {
		log.Error("json.Marshal arg error: %+v", err)
		return nil, err
	}

	session := c.nextSession()
	// 发送请求
	req := RPCMessage{
		Session: session,
		Method:  method,
		Data:    dataBytes,
	}
	b, err := c.marshalRPCMessage(req)
	if err != nil {
		log.Error("marshal rpc arg error: %+v", err)
		return nil, err
	}

	rspChan := make(chan *RPCMessage, 1)
	c.pendingMu.Lock()
	if ctx.Err() != nil {
		c.pendingMu.Unlock()
		return nil, c.callDoneError(ctx, "send request timeout")
	}
	c.pending[session] = rspChan
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, session)
		c.pendingMu.Unlock()
	}()

	// 超时后保留已入队帧的期限，直到写协程取出并跳过它。
	queue.calls.Store(&b[0], ctx)
	select {
	case queue.frames <- b:
	case <-ctx.Done():
		queue.calls.Delete(&b[0])
		return nil, c.callDoneError(ctx, "send request timeout")
	}

	// 等待响应或超时
	select {
	case rsp := <-rspChan:
		if rsp.Error != "" {
			return nil, errors.New(rsp.Error)
		}
		return rsp.Data, nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Error("rpc call timeout, method: %s, session: %s", method, session)
		}
		return nil, c.callDoneError(ctx, "rpc timeout")
	}
}

func (c *WSClient) callContext(connCtx context.Context, method string) (context.Context, context.CancelFunc) {
	timeout := c.callTimeout(method)
	if timeout <= 0 {
		return context.WithCancel(connCtx)
	}
	return context.WithTimeout(connCtx, timeout)
}

func (c *WSClient) callTimeout(method string) time.Duration {
	c.callTimeoutsMu.RLock()
	timeout, ok := c.callTimeouts[method]
	c.callTimeoutsMu.RUnlock()
	if ok {
		return timeout
	}
	return time.Duration(c.defaultCallTimeout.Load())
}

func (c *WSClient) callDoneError(ctx context.Context, timeoutMsg string) error {
	if c.Ctx.Err() != nil {
		return errors.New("client is closing")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return errors.New("connection lost, please retry")
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New(timeoutMsg)
	}
	return ctx.Err()
}

func (c *WSClient) otherResponse(msg *RPCMessage) bool {
	if msg.Session == "" || msg.Method != "" || (len(msg.Data) != 0 && !json.Valid(msg.Data)) {
		return false
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	ch, exists := c.pending[msg.Session]
	if exists {
		// 非阻塞发送，避免接收端已退出导致阻塞
		select {
		case ch <- msg:
		default:
			log.Warn("otherResponse: response channel full or closed for session %s", msg.Session)
		}
		delete(c.pending, msg.Session)
		return true
	}
	return false
}

// meResponse 把服务端请求的处理结果写回收到该请求的连接。sendCh 必须是本次
// 连接的发送队列，不能重新读取客户端当前队列，否则旧连接处理器迟到的响应
// 会被发到重连后的新连接。
func (c *WSClient) meResponse(ctx context.Context, sendCh chan<- []byte, session string, data any, err error) {
	if ctx.Err() != nil || sendCh == nil {
		return
	}

	// 先把业务结果转成字节
	var dataBytes []byte
	if data != nil {
		var marshalErr error
		dataBytes, marshalErr = json.Marshal(data)
		if marshalErr != nil {
			log.Error("meResponse: json.Marshal failed: session=%s, err=%v", session, marshalErr)
			err = fmt.Errorf("marshal response failed: %v", marshalErr)
		}
	}

	rsp := RPCMessage{
		Session: session,
		Data:    dataBytes,
	}
	if err != nil {
		rsp.Error = err.Error()
	}
	b, encodeErr := c.marshalRPCMessage(rsp)
	if encodeErr != nil && (len(rsp.Data) != 0 || rsp.Error == "") {
		log.Error("meResponse: marshal response failed: session=%s, err=%v", session, encodeErr)
		rsp.Data = nil
		if rsp.Error == "" {
			rsp.Error = "encode response failed"
		}
		b, encodeErr = c.marshalRPCMessage(rsp)
	}
	if encodeErr != nil {
		log.Error("meResponse: marshal error response failed: session=%s, err=%v", session, encodeErr)
		return
	}
	select {
	case sendCh <- b:
	case <-time.After(time.Duration(c.writeTimeout.Load())):
		log.Warn("meResponse: send response timeout for session %s", session)
	case <-ctx.Done():
		log.Warn("meResponse: connection closed, drop response for session %s", session)
	}
}

// nextSession 生成客户端发起的 RPC 会话 ID。
// 加 "clt-" 前缀与服务端前缀隔离：两端 nextSession 都是从 1 开始的自增数字，
// 不加前缀时客户端请求 session 可能与服务端请求 session 相同，导致
// otherResponse 把对方的请求误判为响应并吞掉。
func (c *WSClient) nextSession() string {
	return "clt-" + strconv.FormatInt(atomic.AddInt64(&c.sessionSeed, 1), 10)
}

// heartbeatPeriod 返回心跳间隔；非法值回退默认值，避免 time.NewTicker panic。
func (c *WSClient) heartbeatPeriod() time.Duration {
	period := time.Duration(c.heartbeatInterval.Load())
	if period <= 0 {
		return defaultHeartbeatInterval
	}
	return period
}

// dialTimeoutPeriod 返回单次拨号超时；非法值回退默认值。
func (c *WSClient) dialTimeoutPeriod() time.Duration {
	timeout := time.Duration(c.dialTimeout.Load())
	if timeout <= 0 {
		return defaultDialTimeout
	}
	return timeout
}

// NewClient 初始化客户端（在这里绑定 Context）
func NewClient(ctx context.Context, host string, clientType string, clientId string) *WSClient {
	// 创建一个继承自传入 ctx 的子 context
	addr := host
	if !strings.Contains(host, "://") {
		target := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
		addr = target.String()
	}
	cCtx, cCancel := context.WithCancel(ctx)

	c := &WSClient{
		Addr:         addr,
		ClientType:   clientType,
		ClientId:     clientId,
		SendCh:       make(chan []byte, defaultSendQueue),
		Ctx:          cCtx,
		Cancel:       cCancel,
		pending:      make(map[string]chan *RPCMessage),
		handlers:     make(map[string]func(*WSClient, json.RawMessage) (any, error)),
		callTimeouts: make(map[string]time.Duration),
		sessionSeed:  0,
		readLimit:    512 * 1024, // 读限制默认 512KB
	}
	c.handlerQueueSize.Store(defaultHandlerQueue)
	c.HandlerCh = make(chan RPCMessage, c.handlerQueueCap())
	c.writeTimeout.Store(int64(5 * time.Second))
	c.defaultCallTimeout.Store(int64(5 * time.Second))
	c.heartbeatInterval.Store(int64(defaultHeartbeatInterval))
	c.dialTimeout.Store(int64(defaultDialTimeout))
	c.readDeadline.Store(int64(30 * time.Second))
	c.controlWriteTimeout.Store(int64(2 * time.Second))
	c.authPayload = defaultAuthPayload
	log.Info("客户端创建完成: addr=%s, type=%s, id=%s", c.Addr, c.ClientType, c.ClientId)
	return c
}

// Close stops reconnecting and waits up to five seconds for connection pumps to exit.
// A connection still authenticating is closed immediately.
// A handler calling Close initiates shutdown and returns so its pump can exit.
func (c *WSClient) Close() {
	c.closeOnce.Do(func() {
		c.closeDone = make(chan struct{})
		c.mu.Lock()
		c.closed.Store(true)
		authConn := c.authConn
		c.authConn = nil
		pending := make([]<-chan struct{}, 0, len(c.pumpsDone))
		for done := range c.pumpsDone {
			pending = append(pending, done)
		}
		c.mu.Unlock()

		log.Info("正在关闭客户端: addr=%s, type=%s, id=%s", c.Addr, c.ClientType, c.ClientId)
		c.Cancel()
		if authConn != nil {
			_ = authConn.CloseNow()
		}
		go c.waitForClose(pending)
	})
	if _, insideHandler := c.handlerPumps.Load(goid.Get()); !insideHandler {
		<-c.closeDone
	}
}

func (c *WSClient) waitForClose(pending []<-chan struct{}) {
	defer close(c.closeDone)
	defer log.Info("客户端已关闭: addr=%s, type=%s, id=%s", c.Addr, c.ClientType, c.ClientId)
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for _, done := range pending {
		select {
		case <-done:
		case <-timer.C:
			log.Warn("close: wait goroutine exit timeout")
			return
		}
	}
}

// IsReady 返回当前底层连接是否就绪（已连接且未主动关闭）
func (c *WSClient) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isReady && c.Conn != nil && c.connCtx != nil && c.connCtx.Err() == nil && !c.closed.Load()
}

// IsConnected 返回客户端是否处于可用状态：连接就绪且客户端未被关闭
func (c *WSClient) IsConnected() bool {
	return c.IsReady()
}

// cleanPendingWithError 当连接断开时，让所有未完成的 Call 返回错误
func (c *WSClient) cleanPendingWithError(errMsg string) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for session, ch := range c.pending {
		select {
		case ch <- &RPCMessage{Session: session, Error: errMsg}:
		default:
		}
		delete(c.pending, session)
	}
}

// Start 运行连接和重连循环。同一客户端已有循环运行时，重复调用直接返回。
// onConnected 串行异步执行。回调执行期间发生多次重连时，只保留最新会话的待执行通知。
// 回调使用 IsReady、Send、Call 等方法访问当前会话。Conn、IP、Port、SendCh 和 HandlerCh
// 等字段会在重连时更新，运行期间不能直接并发访问。
func (c *WSClient) Start(onConnected func(*WSClient)) {
	if c.closed.Load() || !c.running.CompareAndSwap(false, true) {
		return
	}
	defer c.running.Store(false)

	var connected chan context.Context
	if onConnected != nil {
		connected = make(chan context.Context, 1)
		go c.notifyConnections(connected, onConnected)
		defer close(connected)
	}
	log.Info("客户端运行循环启动: addr=%s, type=%s, id=%s", c.Addr, c.ClientType, c.ClientId)
	defer log.Info("客户端运行循环退出: addr=%s, type=%s, id=%s", c.Addr, c.ClientType, c.ClientId)

	const (
		minBackoff = 1 * time.Second
		maxBackoff = 60 * time.Second
		// 连接存活不足该时长视为失败，避免"认证成功即被断开"造成 1 秒级重连循环。
		stableConnection = 10 * time.Second
	)
	var consecutiveFailures int // 连续失败次数

	// 创建一个局部随机源，避免全局 rand 锁竞争
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-c.Ctx.Done():
			return
		default:
		}

		// 如果有连续失败记录，则先等待退避时间
		if consecutiveFailures > 0 {
			exponent := min(consecutiveFailures-1, 10) // 2^10 * 1s = 1024s，远大于 maxBackoff 60s，所以限制到 10 足够了
			// 计算基础退避时间（指数增长，上限 maxBackoff）
			base := min(minBackoff*(1<<exponent), maxBackoff)
			// 加入随机抖动（0 ~ base/2），使用本地随机源
			maxJitter := base / 2
			if maxJitter <= 0 {
				maxJitter = 1 * time.Millisecond
			}
			jitter := time.Duration(rng.Int63n(int64(maxJitter)))
			backoff := base + jitter

			log.Info("连接失败 %d 次，等待 %v 后重连...", consecutiveFailures, backoff)
			select {
			case <-time.After(backoff):
			case <-c.Ctx.Done():
				return
			}
		}

		log.Info("正在连接: %s", c.Addr)
		dialCtx, dialCancel := context.WithTimeout(c.Ctx, c.dialTimeoutPeriod())
		conn, _, err := websocket.Dial(dialCtx, c.Addr, nil)
		dialCancel()
		if err != nil {
			consecutiveFailures++
			log.Info("连接失败: %v", err)
			continue
		}

		c.mu.Lock()
		if c.closed.Load() || c.Ctx.Err() != nil {
			c.mu.Unlock()
			_ = conn.CloseNow()
			return
		}
		c.authConn = conn
		c.mu.Unlock()

		// 设置读限制
		conn.SetReadLimit(c.readLimit)

		// Payload preparation may block before auth starts any context-aware I/O.
		stopAuthClose := context.AfterFunc(c.Ctx, func() { _ = conn.CloseNow() })
		err = c.auth(conn, time.Duration(c.writeTimeout.Load()))
		stopAuthClose()
		if err != nil {
			_ = conn.CloseNow()
			c.mu.Lock()
			c.authConn = nil
			c.mu.Unlock()
			if errors.Is(err, ErrAuthRejected) {
				log.Error("认证被服务端拒绝: %v，客户端彻底停止", err)
				c.Cancel()
				return
			}
			// 网络抖动、认证响应超时、响应格式错误等临时故障不应永久停机，
			// 计入失败次数后按指数退避重连。
			consecutiveFailures++
			log.Warn("认证过程发生临时故障(将重试): %v", err)
			continue
		}

		// 认证成功，但先不清零失败计数：只有连接稳定存活后才算真正连上。
		connectedAt := time.Now()

		connCtx, connCancel := context.WithCancel(c.Ctx)
		var connWG sync.WaitGroup
		connDone := make(chan struct{})
		handlerCh := make(chan RPCMessage, c.handlerQueueCap())
		queue := &outboundQueue{frames: make(chan []byte, defaultSendQueue), cancel: connCancel}
		sendCh := queue.frames
		connWG.Add(3)

		c.mu.Lock()
		c.authConn = nil
		if c.closed.Load() || connCtx.Err() != nil {
			c.mu.Unlock()
			connCancel()
			_ = conn.CloseNow()
			return
		}
		c.Conn = conn
		if c.pumpsDone == nil {
			c.pumpsDone = make(map[chan struct{}]struct{})
		}
		c.pumpsDone[connDone] = struct{}{}
		c.connCtx = connCtx
		c.HandlerCh = handlerCh
		c.SendCh = sendCh
		c.outbound = queue
		u, parseErr := url.Parse(c.Addr)
		if parseErr == nil {
			host, port, splitErr := net.SplitHostPort(u.Host)
			if splitErr == nil {
				c.IP = host
				if p, convErr := strconv.Atoi(port); convErr == nil {
					c.Port = p
				} else {
					c.Port = -1
				}
			} else {
				c.IP = u.Host
				c.Port = -1
			}
		} else {
			c.IP = c.Addr
			c.Port = -1
		}
		c.isReady = true
		c.mu.Unlock()

		log.Info("已连接到服务器: %s, IP地址：%s:%d", c.Addr, c.IP, c.Port)

		go c.readPump(connCtx, conn, handlerCh, sendCh, queue.stop, &connWG)
		go c.writePump(connCtx, conn, queue, &connWG)
		go c.handlerPump(connCtx, handlerCh, sendCh, &connWG)
		go func() {
			connWG.Wait()
			c.mu.Lock()
			delete(c.pumpsDone, connDone)
			close(connDone)
			c.mu.Unlock()
		}()

		if connected != nil {
			select {
			case <-connected:
			default:
			}
			connected <- connCtx
		}

		<-connCtx.Done()

		// 连接断开，清理所有 pending 请求
		c.cleanPendingWithError("connection lost, please retry")

		c.mu.Lock()
		if c.Conn == conn {
			c.Conn = nil
			c.connCtx = nil
			c.outbound = nil
		}
		c.isReady = false
		c.mu.Unlock()

		select {
		case <-c.Ctx.Done():
			// 彻底关闭，等待协程退出（带超时）
			c.waitDoneWithTimeout(connDone, 2*time.Second, "等待读写协程退出超时")
			log.Info("不再重连：客户端已彻底关闭")
			return
		default:
			if lifetime := time.Since(connectedAt); lifetime >= stableConnection {
				consecutiveFailures = 0
			} else {
				consecutiveFailures++
				log.Warn("连接存活时间过短(%v)，按失败重连处理 (连续失败 %d 次)", lifetime, consecutiveFailures)
			}
			// 等待旧连接协程退出后再重连，增加超时保护
			c.waitDoneWithTimeout(connDone, 2*time.Second, "等待旧连接协程退出超时，强制继续重连")
			log.Info("🔄 连接异常断开，准备重连...")
			// 删除多余的 sleep（指数退避已经处理了等待）
		}
	}
}

func (c *WSClient) notifyConnections(connected <-chan context.Context, onConnected func(*WSClient)) {
	for connCtx := range connected {
		if connCtx.Err() != nil {
			continue
		}
		func() {
			defer func() {
				if value := recover(); value != nil {
					log.Error("连接回调 panic: %v", value)
				}
			}()
			onConnected(c)
		}()
	}
}

// auth 在给定连接上执行认证，超时时间内等待服务端返回 AuthResponse。
// 如果认证成功返回 nil，否则返回错误（调用方应关闭连接并重试）
func (c *WSClient) auth(conn *websocket.Conn, timeout time.Duration) error {
	authData, err := c.buildAuthData()
	if err != nil {
		return err
	}

	writeCtx, writeCancel := context.WithTimeout(c.Ctx, time.Duration(c.writeTimeout.Load()))
	err = conn.Write(writeCtx, websocket.MessageText, authData)
	writeCancel()
	if err != nil {
		return fmt.Errorf("write auth message failed: %w", err)
	}

	readCtx, readCancel := context.WithTimeout(c.Ctx, timeout)
	_, data, err := conn.Read(readCtx)
	readCancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.New("auth timeout")
		}
		// 服务端可能在发送 AuthResponse 前直接以 1008 关闭连接（旧实现或中间
		// 代理均可能如此），这属于明确的认证拒绝，不能当作临时故障无限重连。
		if websocket.CloseStatus(err) == websocket.StatusPolicyViolation {
			return fmt.Errorf("%w: connection closed by server", ErrAuthRejected)
		}
		return fmt.Errorf("read auth response failed: %w", err)
	}

	var resp AuthResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("invalid auth response: %w", err)
	}
	if !resp.OK {
		if resp.Error == "" {
			return fmt.Errorf("%w: auth failed", ErrAuthRejected)
		}
		return fmt.Errorf("%w: %s", ErrAuthRejected, resp.Error)
	}
	return nil
}

func (c *WSClient) waitDoneWithTimeout(done <-chan struct{}, timeout time.Duration, warnMsg string) {
	if done == nil {
		return
	}
	select {
	case <-done:
		// 正常退出
	case <-time.After(timeout):
		log.Warn("timeout exit: %v", warnMsg)
	}
}

func (c *WSClient) WaitStopSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	select {
	case <-stop:
		log.Info("收到退出信号，正在关闭 WebSocket 资源...")
	case <-c.Ctx.Done():
		log.Info("客户端已内部关闭，程序退出...")
	}
	c.Close()
	log.Info("资源清理完毕")
}

// SetReadDeadline 已废弃且不生效：coder/websocket 不支持单连接读超时，
// 读操作只受连接 context 约束。保留该方法仅为兼容历史调用。
func (c *WSClient) SetReadDeadline(d time.Duration) {
	c.readDeadline.Store(int64(d))
}

// SetHeartbeatInterval 设置 Ping 心跳发送间隔，应在 Start 前调用。
// 每次建立连接时按当前值创建心跳 ticker，非正值会被忽略。
func (c *WSClient) SetHeartbeatInterval(d time.Duration) {
	if d <= 0 {
		log.Warn("忽略非法心跳间隔: %v", d)
		return
	}
	c.heartbeatInterval.Store(int64(d))
}

// SetDialTimeout 设置单次 WebSocket 拨号（TCP 连接 + TLS + 握手）超时，
// 非正值会被忽略。运行期间修改从下一次拨号开始生效。
func (c *WSClient) SetDialTimeout(d time.Duration) {
	if d <= 0 {
		log.Warn("忽略非法拨号超时: %v", d)
		return
	}
	c.dialTimeout.Store(int64(d))
}

func (c *WSClient) SetControlWriteTimeout(d time.Duration) {
	c.controlWriteTimeout.Store(int64(d))
}

func (c *WSClient) SetDefaultCallTimeout(d time.Duration) {
	c.defaultCallTimeout.Store(int64(d))
}

func (c *WSClient) SetCallTimeout(method string, d time.Duration) {
	c.callTimeoutsMu.Lock()
	defer c.callTimeoutsMu.Unlock()
	c.callTimeouts[method] = d
}

func (c *WSClient) SetCallTimeouts(timeouts map[string]time.Duration) {
	c.callTimeoutsMu.Lock()
	defer c.callTimeoutsMu.Unlock()
	for method, timeout := range timeouts {
		c.callTimeouts[method] = timeout
	}
}

func (c *WSClient) SetHandlerQueueSize(n int) {
	if n <= 0 {
		return
	}
	c.handlerQueueSize.Store(int64(n))
	c.mu.Lock()
	if !c.isReady {
		c.HandlerCh = make(chan RPCMessage, n)
	}
	c.mu.Unlock()
}

func (c *WSClient) SetAuthPayload(fn AuthPayloadFunc) {
	if fn == nil {
		log.Warn("忽略空的认证载荷函数: client=%s", c.ClientId)
		return
	}
	c.mu.Lock()
	c.authPayload = fn
	c.mu.Unlock()
	log.Info("已配置自定义认证载荷: client=%s", c.ClientId)
}

func (c *WSClient) buildAuthData() ([]byte, error) {
	c.mu.RLock()
	fn := c.authPayload
	c.mu.RUnlock()
	if fn == nil {
		fn = defaultAuthPayload
	}

	payload, err := fn(c)
	if err != nil {
		return nil, fmt.Errorf("build auth payload failed: %w", err)
	}
	switch v := payload.(type) {
	case []byte:
		return v, nil
	case json.RawMessage:
		return []byte(v), nil
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal auth payload failed: %w", err)
		}
		return b, nil
	}
}

func defaultAuthPayload(c *WSClient) (any, error) {
	return AuthMessage{
		Type: c.ClientType,
		ID:   c.ClientId,
	}, nil
}

func (c *WSClient) enqueueTextMessage(ctx context.Context, handlerCh chan RPCMessage, sendCh chan []byte, msg RPCMessage) {
	select {
	case handlerCh <- msg:
	default:
		err := fmt.Errorf("handler queue full")
		log.Warn("drop request: %v, method=%s, session=%s", err, msg.Method, msg.Session)
		if msg.Session != "" {
			c.meResponse(ctx, sendCh, msg.Session, nil, err)
		}
	}
}

func (c *WSClient) handlerPump(ctx context.Context, handlerCh <-chan RPCMessage, sendCh chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	id := goid.Get()
	c.handlerPumps.Store(id, struct{}{})
	defer c.handlerPumps.Delete(id)
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-handlerCh:
			if ctx.Err() != nil {
				return
			}
			c.dispatchTextMessage(ctx, sendCh, msg)
		}
	}
}

func (c *WSClient) handlerQueueCap() int {
	n := c.handlerQueueSize.Load()
	if n <= 0 {
		return defaultHandlerQueue
	}
	return int(n)
}

func (c *WSClient) dispatchTextMessage(ctx context.Context, sendCh chan []byte, msg RPCMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panic recovered: method=%s, panic=%v", msg.Method, r)
			if msg.Session != "" {
				c.meResponse(ctx, sendCh, msg.Session, nil, fmt.Errorf("handler panic: %v", r))
			}
		}
	}()

	rsp, err := c.handleTextMessage(msg.Method, msg.Data)
	if ctx.Err() != nil {
		log.Warn("drop response for closed connection, method=%s, session=%s", msg.Method, msg.Session)
		return
	}
	if msg.Session != "" {
		c.meResponse(ctx, sendCh, msg.Session, &rsp, err)
	}
}

func (c *WSClient) handleTextMessage(method string, data json.RawMessage) (result any, err error) {
	c.handlersMu.RLock()
	handler, exists := c.handlers[method]
	c.handlersMu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no this handler: %s", method)
	}

	// 增加 panic 恢复机制
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panic recovered: method=%s, panic=%v", method, r)
			// 可以选择打印调用栈
			// debug.PrintStack()
			err = fmt.Errorf("handler panic: %v", r)
			result = nil
		}
	}()
	return handler(c, data)
}

// RegisterHandler 注册业务处理函数
func (c *WSClient) RegisterHandler(method string, handler func(clt *WSClient, data json.RawMessage) (any, error)) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	c.handlers[method] = handler
	log.Info("已注册客户端处理器: client=%s, method=%s", c.ClientId, method)
}

func CallT[T any](c *WSClient, method string, arg any) (*T, json.RawMessage, *WSClient, error) {
	b, err := c.Call(method, arg)
	if err != nil {
		log.Error("call error : %v", err)
		return nil, nil, nil, err
	}

	var data T
	if err := json.Unmarshal(b, &data); err != nil {
		log.Error("json.Unmarshal error: %+v", err)
		return nil, nil, c, fmt.Errorf("unmarshal response failed: %w", err)
	}

	return &data, b, c, nil
}
