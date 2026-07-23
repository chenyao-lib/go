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
)

type RPCMessage struct {
	Session string          `json:"session"` // 唯一会话 ID
	Method  string          `json:"method"`  // 调用的方法名
	Data    json.RawMessage `json:"data"`    // 请求参数或响应数据
	Error   string          `json:"error"`   // 错误信息
}

const defaultHandlerQueue = 256

type AuthPayloadFunc func(c *WSClient) (any, error)

type WSClient struct {
	Addr       string
	ClientType string
	ClientId   string
	Conn       *websocket.Conn
	IP         string
	Port       int
	SendCh     chan []byte
	Ctx        context.Context
	Cancel     context.CancelFunc
	mu         sync.RWMutex
	isReady    bool
	connDone   <-chan struct{}
	codec      payloadcodec.Codec

	pending     map[string]chan *RPCMessage
	pendingMu   sync.Mutex
	sessionSeed int64

	// handlers 存储 method 到 函数的映射
	handlers         map[string]func(clt *WSClient, data json.RawMessage) (any, error)
	handlersMu       sync.RWMutex
	HandlerCh        chan RPCMessage
	handlerQueueSize atomic.Int64

	closed              atomic.Bool  // 标记是否已彻底关闭
	readLimit           int64        // 读大小限制
	writeTimeout        atomic.Int64 // 写超时，默认5秒
	defaultCallTimeout  atomic.Int64 // Call 默认超时，默认5秒
	heartbeatInterval   atomic.Int64 // 心跳发送间隔，默认 20 秒
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

func (c *WSClient) readPump(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc, wg *sync.WaitGroup) {
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
			if c.otherResponse(&msg) {
				continue
			}
			if msg.Method == "" {
				log.Error("没有处理方法: %s", string(data))
				continue
			}
			c.enqueueTextMessage(ctx, msg)

		case websocket.MessageBinary:
			log.Info("收到二进制，大小为: %d 字节", len(data))
		}
	}
}

func (c *WSClient) writePump(ctx context.Context, conn *websocket.Conn, wg *sync.WaitGroup) {
	ticker := time.NewTicker(time.Duration(c.heartbeatInterval.Load()))
	defer func() {
		ticker.Stop()
		conn.CloseNow()
		wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			// 退出前优雅关闭协议连接
			_ = conn.Close(websocket.StatusNormalClosure, "")
			return
		case msg, ok := <-c.SendCh:
			if !ok {
				return
			}
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

func (c *WSClient) Send(method string, arg any) error {
	// 连接状态检查
	if c.closed.Load() {
		return errors.New("client already closed")
	}
	c.mu.RLock()
	ready := c.isReady && c.Conn != nil
	c.mu.RUnlock()
	if !ready {
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

	select {
	case c.SendCh <- b:
		return nil
	default:
		// 通道满，再检查是否因为 ctx 取消导致
		select {
		case <-c.Ctx.Done():
			return errors.New("client is closing")
		default:
			return errors.New("send channel is full, message dropped")
		}
	}
}

func (c *WSClient) Call(method string, arg any) (json.RawMessage, error) {
	// 如果客户端已关闭或连接未就绪，直接返回错误
	if c.closed.Load() {
		return nil, errors.New("client already closed")
	}
	c.mu.RLock()
	ready := c.isReady && c.Conn != nil
	c.mu.RUnlock()
	if !ready {
		return nil, errors.New("connection is not ready, reconnecting")
	}

	ctx, cancel := c.callContext(method)
	defer cancel()

	// 序列化参数
	dataBytes, err := json.Marshal(arg)
	if err != nil {
		log.Error("json.Marshal arg error: %+v", err)
		return nil, err
	}

	session := c.nextSession()
	rspChan := make(chan *RPCMessage, 1)

	// 注册等待队列
	c.pendingMu.Lock()
	c.pending[session] = rspChan
	c.pendingMu.Unlock()

	// 确保退出时清理
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, session)
		c.pendingMu.Unlock()
	}()

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

	select {
	case c.SendCh <- b:
	case <-ctx.Done():
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
		log.Error("rpc call timeout, data: %s", string(b))
		return nil, c.callDoneError(ctx, "rpc timeout")
	}
}

func (c *WSClient) callContext(method string) (context.Context, context.CancelFunc) {
	timeout := c.callTimeout(method)
	if timeout <= 0 {
		return context.WithCancel(c.Ctx)
	}
	return context.WithTimeout(c.Ctx, timeout)
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
	if errors.Is(c.Ctx.Err(), context.Canceled) {
		return errors.New("client is closing")
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New(timeoutMsg)
	}
	return ctx.Err()
}

func (c *WSClient) otherResponse(msg *RPCMessage) bool {
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

func (c *WSClient) meResponse(session string, data any, err error) {
	// 如果客户端已彻底关闭，直接返回，避免向已关闭的 SendCh 发送
	if c.closed.Load() {
		return
	}

	c.mu.RLock()
	ready := c.isReady && c.Conn != nil
	c.mu.RUnlock()
	if !ready {
		log.Warn("meResponse: connection not ready, drop response for session %s", session)
		return
	}

	// 先把业务结果转成字节
	var dataBytes []byte
	if data != nil {
		var marshalErr error
		dataBytes, marshalErr = json.Marshal(data)
		if marshalErr != nil {
			log.Error("meResponse: json.Marshal failed: %v", marshalErr)
			// 发送错误响应
			errRsp := RPCMessage{
				Session: session,
				Error:   fmt.Sprintf("marshal response failed: %v", marshalErr),
			}
			b, encodeErr := c.marshalRPCMessage(errRsp)
			if encodeErr != nil {
				log.Error("meResponse: marshal error response failed: %v", encodeErr)
				return
			}
			select {
			case c.SendCh <- b:
			case <-time.After(time.Duration(c.writeTimeout.Load())):
				log.Warn("meResponse: send response timeout for session %s", session)
			case <-c.Ctx.Done():
				log.Warn("meResponse: client closing, drop response for session %s", session)
			}
			return
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
	if encodeErr != nil {
		log.Error("meResponse: marshal response failed: %v", encodeErr)
		return
	}
	select {
	case c.SendCh <- b:
	case <-time.After(time.Duration(c.writeTimeout.Load())):
		log.Warn("meResponse: send response timeout for session %s", session)
	case <-c.Ctx.Done():
		log.Warn("meResponse: client closing, drop response for session %s", session)
	}
}

func (c *WSClient) nextSession() string {
	return strconv.FormatInt(atomic.AddInt64(&c.sessionSeed, 1), 10)
}

// NewClient 初始化客户端（在这里绑定 Context）
func NewClient(ctx context.Context, host string, clientType string, clientId string) *WSClient {
	// 创建一个继承自传入 ctx 的子 context
	var addr string
	if strings.Contains(host, "://") {
		addr = host
	} else {
		addr = fmt.Sprintf("ws://%s/ws", host) // 自动补全
	}
	cCtx, cCancel := context.WithCancel(ctx)

	c := &WSClient{
		Addr:         addr,
		ClientType:   clientType,
		ClientId:     clientId,
		SendCh:       make(chan []byte, 100),
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
	c.heartbeatInterval.Store(int64(20 * time.Second))
	c.readDeadline.Store(int64(30 * time.Second))
	c.controlWriteTimeout.Store(int64(2 * time.Second))
	c.authPayload = defaultAuthPayload
	return c
}

// ========== 修改后 ==========
func (c *WSClient) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.Cancel()
	c.mu.RLock()
	done := c.connDone
	c.mu.RUnlock()
	c.waitDoneWithTimeout(done, 5*time.Second, "close: wait goroutine exit timeout")
}

// IsReady 返回当前底层连接是否就绪（已连接且未主动关闭）
func (c *WSClient) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isReady
}

// IsConnected 返回客户端是否处于可用状态：连接就绪且客户端未被关闭
func (c *WSClient) IsConnected() bool {
	if c.closed.Load() {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isReady && c.Conn != nil
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

func (c *WSClient) Start(onConnected func(*WSClient)) {
	const (
		minBackoff = 1 * time.Second
		maxBackoff = 60 * time.Second
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
		conn, _, err := websocket.Dial(c.Ctx, c.Addr, nil)
		if err != nil {
			consecutiveFailures++
			log.Info("连接失败: %v", err)
			continue
		}

		// 设置读限制和读超时
		conn.SetReadLimit(c.readLimit)

		if err := c.auth(conn, time.Duration(c.writeTimeout.Load())); err != nil {
			log.Error("认证失败: %v，客户端彻底停止", err)
			conn.CloseNow()
			c.Cancel()
			return
		}

		// 连接成功，重置失败计数
		consecutiveFailures = 0

		connCtx, connCancel := context.WithCancel(c.Ctx)
		var connWG sync.WaitGroup
		connDone := make(chan struct{})
		handlerCh := make(chan RPCMessage, c.handlerQueueCap())
		connWG.Add(2)

		c.mu.Lock()
		c.Conn = conn
		c.connDone = connDone
		c.HandlerCh = handlerCh
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

		go c.readPump(connCtx, conn, connCancel, &connWG)
		go c.writePump(connCtx, conn, &connWG)
		go c.handlerPump(connCtx, handlerCh)
		go func() {
			connWG.Wait()
			close(connDone)
		}()

		// 认证成功，调用回调
		if onConnected != nil {
			onConnected(c)
		}

		<-connCtx.Done()

		// 连接断开，清理所有 pending 请求
		c.cleanPendingWithError("connection lost, please retry")

		c.mu.Lock()
		if c.Conn == conn {
			c.Conn = nil
			c.connDone = nil
		}
		c.isReady = false
		c.mu.Unlock()

		for len(c.SendCh) > 0 {
			<-c.SendCh
		}

		select {
		case <-c.Ctx.Done():
			// 彻底关闭，等待协程退出（带超时）
			c.waitDoneWithTimeout(connDone, 2*time.Second, "等待读写协程退出超时")
			log.Info("不再重连：客户端已彻底关闭")
			return
		default:
			// 等待旧连接协程退出后再重连，增加超时保护
			c.waitDoneWithTimeout(connDone, 2*time.Second, "等待旧连接协程退出超时，强制继续重连")
			log.Info("🔄 连接异常断开，准备重连...")
			// 删除多余的 sleep（指数退避已经处理了等待）
		}
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
		return fmt.Errorf("read auth response failed: %w", err)
	}

	var resp AuthResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("invalid auth response: %w", err)
	}
	if !resp.OK {
		if resp.Error == "" {
			return errors.New("auth failed")
		}
		return errors.New(resp.Error)
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

	select {
	case <-stop:
		log.Info("收到退出信号，正在关闭 WebSocket 资源...")
	case <-c.Ctx.Done():
		log.Info("客户端已内部关闭，程序退出...")
	}
	c.Close()
	log.Info("资源清理完毕")
}

func (c *WSClient) SetReadDeadline(d time.Duration) {
	c.readDeadline.Store(int64(d))
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
		return
	}
	c.mu.Lock()
	c.authPayload = fn
	c.mu.Unlock()
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

func (c *WSClient) enqueueTextMessage(ctx context.Context, msg RPCMessage) {
	c.mu.RLock()
	handlerCh := c.HandlerCh
	c.mu.RUnlock()

	select {
	case handlerCh <- msg:
	default:
		err := fmt.Errorf("handler queue full")
		log.Warn("drop request: %v, method=%s, session=%s", err, msg.Method, msg.Session)
		if msg.Session != "" && ctx.Err() == nil {
			c.meResponse(msg.Session, nil, err)
		}
	}
}

func (c *WSClient) handlerPump(ctx context.Context, handlerCh <-chan RPCMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-handlerCh:
			c.dispatchTextMessage(ctx, msg)
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

func (c *WSClient) dispatchTextMessage(ctx context.Context, msg RPCMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panic recovered: method=%s, panic=%v", msg.Method, r)
			if msg.Session != "" && ctx.Err() == nil {
				c.meResponse(msg.Session, nil, fmt.Errorf("handler panic: %v", r))
			}
		}
	}()

	rsp, err := c.handleTextMessage(msg.Method, msg.Data)
	if ctx.Err() != nil {
		log.Warn("drop response for closed connection, method=%s, session=%s", msg.Method, msg.Session)
		return
	}
	if msg.Session != "" {
		c.meResponse(msg.Session, &rsp, err)
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
