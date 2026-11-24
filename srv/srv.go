// Package srv 提供带认证、客户端分组、双向 RPC 和优雅关闭的 WebSocket 服务端。
// 服务固定在 /ws 路径接受连接。
//
// # 启动服务
//
//	server := srv.NewServer(":8080")
//	server.SetOriginPatterns("https://example.com")
//	server.SetReadLimit(32 * 1024)
//	server.SetHandlerQueueSize(128)
//	server.SetRateLimit(100, 256 * 1024)
//
//	server.RegisterHandler("worker", "job.run", func(
//		client *srv.Client,
//		data json.RawMessage,
//	) (any, error) {
//		var req RunRequest
//		if err := json.Unmarshal(data, &req); err != nil {
//			return nil, err
//		}
//		return RunResponse{Accepted: true}, nil
//	})
//
//	go func() {
//		if err := server.Start(); err != nil {
//			log.Error("server stopped: %v", err)
//		}
//	}()
//
// Start 会阻塞到 HTTP 服务退出，通常在 goroutine 中启动。程序退出时使用带
// 截止时间的 context 调用 Shutdown：
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	_ = server.Shutdown(ctx)
//
// # 认证
//
// 默认认证器接受包含 type 和 id 的 AuthMessage。生产环境可调用
// SetAuthenticator 校验令牌并返回规范化后的客户端类型、ID 和可选 Codec：
//
//	server.SetAuthenticator(func(
//		ctx context.Context,
//		client *srv.Client,
//		data []byte,
//	) (*srv.AuthResult, error) {
//		// 校验 data 后返回身份；返回错误会拒绝连接。
//		return &srv.AuthResult{Type: "worker", ID: "worker-01"}, nil
//	})
//
// AuthResult.Codec 非 nil 时只加密 RPCMessage.Data，协议外层仍为 JSON。客户端
// 必须配置兼容的 payloadcodec.Codec。
//
// # 消息路由和客户端操作
//
// RegisterHandler 按 clientType 和 method 路由；RegisterDefaultHandler 处理
// 未注册方法。对于已连接客户端，可通过 GetClientByID、WalkClient 或
// SendByID 查找和推送；Client.Call/CallT 可由服务端反向调用客户端并等待响应。
// 同 ID 的新连接完成认证后会替换旧会话，旧会话会收到保留事件 session.kicked。
//
// SetOnClientReady 和 SetOnClientClose 注册连接生命周期回调。使用
// srvsvcbinding 时不要再覆盖这两个回调或默认 Handler，因为 binding 会接管它们。
// OnClientClose 在读取循环退出后立即执行，此时业务 Handler 可能仍在收尾。
//
// NewServer 为兼容旧行为默认允许所有 Origin。对外服务应显式调用
// SetOriginPatterns 设置可信来源。
package srv

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyao-lib/go/payloadcodec"

	"github.com/chenyao-lib/go/log"

	"github.com/coder/websocket"
)

const (
	writeWait                = 10 * time.Second
	authWait                 = 10 * time.Second
	defaultHeartbeatInterval = 9 * time.Second
	defaultReadLimit         = 32 * 1024
	defaultHandlerQueue      = 128
	defaultMessagesPerSec    = 100
	defaultBytesPerSec       = 256 * 1024
)

// WsServer 代表一个独立的 WebSocket 服务实例
type WsServer struct {
	WsPort  string
	connMgr *ConnManager

	clientsMu     sync.RWMutex // 保护连接索引和待就绪的认证顺序
	clientPool    map[string]map[string]*Client
	clientIDMap   map[string]*Client
	sessionOrders map[string]*sessionOrder

	handlers       map[string]map[string]Handler
	defaultHandler Handler
	handlersMu     sync.RWMutex

	lifecycleMu   sync.Mutex
	httpServer    *http.Server
	stopping      bool
	connections   map[*Client]struct{}
	connectionsWG sync.WaitGroup
	shutdownDone  chan struct{}
	shutdownErr   error
	shutdownGrace time.Duration
	forceOnce     sync.Once

	acceptOptions     websocket.AcceptOptions
	handlerQueue      int
	heartbeatInterval time.Duration
	readLimit         int64
	messageRate       int
	byteRate          int64
	authenticator     Authenticator
	onClientReady     ClientHook
	onClientClose     ClientHook
}

type AuthResult struct {
	Type  string
	ID    string
	Codec payloadcodec.Codec
}

type Authenticator func(ctx context.Context, c *Client, data []byte) (*AuthResult, error)
type ClientHook func(c *Client)

type sessionOrder struct {
	lastAuthenticated uint64
	lastReady         uint64
	pending           int
}

// SetOriginPatterns 配置允许跨域访问 WebSocket 的 Origin 白名单。
// 不配置时默认允许所有 Origin，以保持 NewServer 的开箱即用行为。
func (s *WsServer) SetOriginPatterns(patterns ...string) {
	s.acceptOptions.OriginPatterns = append([]string(nil), patterns...)
	s.acceptOptions.InsecureSkipVerify = false
}

// SetReadLimit 设置单条 WebSocket 消息及解码后的 RPC 内容的字节上限，默认 32 KiB。
// 在 Start 前调用。无效值会被忽略。
func (s *WsServer) SetReadLimit(n int64) {
	if n > 0 && n < math.MaxInt64 {
		s.readLimit = n
	}
}

// SetRateLimit 设置每条已认证连接的接收速率，默认每秒 100 条、256 KiB。
// 突发额度为一秒的速率，字节额度至少能容纳一条达到读上限的消息。
// 超限时以 1013 关闭连接。在 Start 前调用，两个参数均须为正数。
func (s *WsServer) SetRateLimit(messagesPerSecond int, bytesPerSecond int64) {
	if messagesPerSecond > 0 && bytesPerSecond > 0 {
		s.messageRate = messagesPerSecond
		s.byteRate = bytesPerSecond
	}
}

// SetHandlerQueueSize 设置每连接业务队列容量，默认 128 条。在 Start 前调用。
func (s *WsServer) SetHandlerQueueSize(n int) {
	if n > 0 {
		s.handlerQueue = n
	}
}

// SetHeartbeatInterval 设置服务端发送 Ping 的间隔，默认 9 秒。
// 在 Start 前调用。非正值会被忽略。
func (s *WsServer) SetHeartbeatInterval(interval time.Duration) {
	if interval > 0 {
		s.heartbeatInterval = interval
	}
}

func (s *WsServer) SetAuthenticator(authenticator Authenticator) {
	if authenticator != nil {
		s.authenticator = authenticator
	}
}

// SetOnClientReady 注册认证成功回调。回调完成后才发布连接、替换旧会话和分发业务消息。
// 回调内请使用传入的 c.Send 给本次连接发消息，消息会在连接就绪后写出。
// 此时 GetClientByID 和 SendByID 查找的是已就绪的会话，可能仍指向旧连接。
// 回调可调用 CloseWithReason 拒绝新连接。Shutdown 会等待回调返回。
func (s *WsServer) SetOnClientReady(fn ClientHook) {
	s.onClientReady = fn
}

// SetOnClientClose 注册连接关闭回调。回调在读取循环退出、连接已从索引移除后
// 同步执行；此时连接 Context 已取消，但业务 Handler 可能仍在收尾，不要假设
// 所有业务 goroutine 都已退出。
func (s *WsServer) SetOnClientClose(fn ClientHook) {
	s.onClientClose = fn
}

// --- 核心结构 ---

type ConnManager struct {
	Clients map[*websocket.Conn]*Client
	Lock    sync.RWMutex
}

type RPCMessage struct {
	Session string          `json:"session"` // 唯一会话 ID
	Method  string          `json:"method"`  // 调用的方法名
	Data    json.RawMessage `json:"data"`    // 请求参数或响应数据
	Error   string          `json:"error"`   // 错误信息
}

type AuthMessage struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type AuthResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type Handler func(clt *Client, msg RPCMessage) (any, error)

type Client struct {
	Conn         *websocket.Conn
	Server       *WsServer // 反向引用所属的 Server 实例
	transport    net.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	closeDone    chan struct{}
	IP           string
	Port         int
	SendCh       chan []byte
	Type         string //客户端类型
	ID           string //客户端ID，全局唯一
	codec        payloadcodec.Codec
	authID       string
	authSequence uint64

	pending    map[string]chan *RPCMessage
	pendingMu  sync.Mutex
	closing    atomic.Bool // 确保只由一个协程关闭连接
	sendMu     sync.RWMutex
	sendClosed bool
	HandlerCh  chan RPCMessage
}

// Context 返回连接的上下文。连接关闭或被强制断开时，上下文会被取消。
func (c *Client) Context() context.Context {
	return c.ctx
}

// --- 逻辑实现 ---

func (m *ConnManager) register(client *Client) {
	m.Lock.Lock()
	m.Clients[client.Conn] = client
	count := len(m.Clients)
	m.Lock.Unlock()
	log.Info("新连接接入, IP：%s:%d，当前总连接数: %d", client.IP, client.Port, count)
}

func (m *ConnManager) unregister(client *Client) {
	m.Lock.Lock()
	if _, ok := m.Clients[client.Conn]; !ok {
		m.Lock.Unlock()
		return
	}
	delete(m.Clients, client.Conn)
	count := len(m.Clients)
	client.sendMu.Lock()
	client.sendClosed = true
	close(client.SendCh)
	client.sendMu.Unlock()
	m.Lock.Unlock()
	log.Info("连接断开, IP: %s:%d，剩余连接数: %d", client.IP, client.Port, count)
}

// 外部调用的 Send 函数保持不变...
func (c *Client) Send(method string, arg any) error {
	if c.closing.Load() {
		return errors.New("client connection closed")
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

	c.sendMu.RLock()
	defer c.sendMu.RUnlock()
	if c.sendClosed || c.closing.Load() {
		return errors.New("client connection closed")
	}
	select {
	case c.SendCh <- b:
		return nil
	default:
		return errors.New("send channel is full, message dropped")
	}
}

func (c *Client) Call(method string, arg any, timeout time.Duration) (json.RawMessage, error) {
	if c.closing.Load() {
		return nil, errors.New("client connection closed")
	}
	// 序列化参数
	dataBytes, err := json.Marshal(arg)
	if err != nil {
		log.Error("json.Marshal arg error: %+v", err)
		return nil, err
	}

	// 保留请求方向前缀，用密码学随机字符串防止对端提前猜中会话。
	session := "srv-" + rand.Text()
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

	c.sendMu.RLock()
	if c.sendClosed || c.closing.Load() {
		c.sendMu.RUnlock()
		return nil, errors.New("client connection closed")
	}
	select {
	case c.SendCh <- b:
	case <-c.ctx.Done():
		c.sendMu.RUnlock()
		return nil, errors.New("client connection closed")
	case <-time.After(writeWait):
		c.sendMu.RUnlock()
		return nil, errors.New("send timeout")
	}
	c.sendMu.RUnlock()

	// 等待响应或超时
	select {
	case rsp := <-rspChan:
		if rsp.Error != "" {
			return nil, errors.New(rsp.Error)
		}
		return rsp.Data, nil
	case <-time.After(timeout):
		log.Error("rpc call timeout, data: %s", string(b))
		return nil, errors.New("rpc timeout")
	}
}

func (c *Client) otherResponse(msg *RPCMessage) bool {
	if msg.Session == "" || msg.Method != "" || (len(msg.Data) != 0 && !json.Valid(msg.Data)) {
		return false
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	ch, exists := c.pending[msg.Session]
	if !exists {
		return false
	}
	select {
	case ch <- msg:
	default:
		log.Warn("otherResponse: receiver gone for session %s", msg.Session)
	}
	delete(c.pending, msg.Session)
	return true
}

func (c *Client) meResponse(session string, data any, err error) {
	if c.closing.Load() {
		log.Warn("meResponse: client already closed, drop response for session %s", session)
		return
	}
	// 1. 先把业务结果转成字节
	var dataBytes []byte
	if data != nil {
		dataBytes, _ = json.Marshal(data)
	}

	rsp := RPCMessage{
		Session: session,
		Data:    dataBytes,
	}
	if err != nil {
		rsp.Error = err.Error()
	}
	b, marshalErr := c.marshalRPCMessage(rsp)
	if marshalErr != nil {
		log.Error("marshal rpc response failed: session=%s, err=%v", session, marshalErr)
		return
	}
	c.sendMu.RLock()
	if c.sendClosed || c.closing.Load() {
		c.sendMu.RUnlock()
		log.Warn("meResponse: client already closed, drop response for session %s", session)
		return
	}
	select {
	case c.SendCh <- b:
	case <-c.ctx.Done():
	case <-time.After(writeWait):
		log.Warn("meResponse: send timeout, drop response for session %s", session)
	}
	c.sendMu.RUnlock()
}

// --- 读写泵 (Pumps) ---

func (c *Client) readPump(readLimit int64, limiter inboundLimiter) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("readPump panic recovered: %v, client %s:%d", r, c.IP, c.Port)
		}
		c.closeNow()
	}()

	for {
		msgType, data, err := c.Conn.Read(c.ctx)
		if err != nil {
			if isNormalReadClose(err) {
				log.Info("readPump exit, IP: %s:%d", c.IP, c.Port)
			} else {
				log.Info("readPump exit, IP: %s:%d, err: %v", c.IP, c.Port, err)
			}
			break
		}

		if !limiter.allow(time.Now(), len(data)) {
			c.CloseWithReason(websocket.StatusTryAgainLater, "message rate limit exceeded")
			return
		}
		if c.closing.Load() {
			continue
		}

		switch msgType {
		case websocket.MessageText:
			var msg RPCMessage
			err := json.Unmarshal(data, &msg)
			if err != nil {
				log.Info("收到非法消息: %s", string(data))
				continue
			}
			if err := c.decodeRPCMessage(&msg, readLimit); err != nil {
				log.Warn("拒绝解码后的客户端消息: client=%s, method=%s, session=%s, err=%v",
					c.ID, msg.Method, msg.Session, err)
				continue
			}
			if c.otherResponse(&msg) {
				continue
			}
			if msg.Method == "" {
				// 既不是 pending 中等待的响应，也没有 Method 字段，说明是无效消息或超时残余
				log.Warn("收到无方法名的消息且无等待者，忽略: session=%s", msg.Session)
				continue
			}
			c.enqueueTextMessage(msg)

		case websocket.MessageBinary:
			log.Info("收到二进制，大小为: %d 字节", len(data))
		}
	}
}

func isNormalReadClose(err error) bool {
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway, websocket.StatusNoStatusRcvd:
		return true
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed)
}

func (c *Client) writePump() {
	ticker := time.NewTicker(c.Server.heartbeatInterval)
	defer func() {
		if r := recover(); r != nil {
			log.Error("writePump panic recovered: %v, client %s:%d", r, c.IP, c.Port)
		}
		ticker.Stop()
		c.closeNow()
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case message, ok := <-c.SendCh:
			if !ok || c.closing.Load() {
				return
			}
			if err := c.writeText(message, writeWait); err != nil {
				return
			}

		case <-ticker.C:
			if c.closing.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(c.ctx, writeWait)
			err := c.Conn.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func NewServer(wsPort string) (s *WsServer) {
	s = &WsServer{
		WsPort:        wsPort,
		clientPool:    make(map[string]map[string]*Client),
		clientIDMap:   make(map[string]*Client),
		sessionOrders: make(map[string]*sessionOrder),
		handlers:      make(map[string]map[string]Handler),
		connections:   make(map[*Client]struct{}),
		shutdownDone:  make(chan struct{}),
		shutdownGrace: defaultShutdownGracePeriod,
		acceptOptions: websocket.AcceptOptions{
			InsecureSkipVerify: true,
		},
		handlerQueue:      defaultHandlerQueue,
		heartbeatInterval: defaultHeartbeatInterval,
		readLimit:         defaultReadLimit,
		messageRate:       defaultMessagesPerSec,
		byteRate:          defaultBytesPerSec,
		authenticator:     defaultAuthenticator,
	}
	// 直接初始化结构体
	mgr := &ConnManager{
		Clients: make(map[*websocket.Conn]*Client),
	}
	s.connMgr = mgr
	// 默认 InsecureSkipVerify=true 会接受任意 Origin；保持兼容但明确提示，
	// 避免对外服务在无感知的情况下被任意站点跨域连接。
	log.Warn("WebSocket Origin 白名单为空，默认允许所有来源；对外服务请调用 SetOriginPatterns: addr=%s", wsPort)
	return s
}

func (s *WsServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.serveWebSocket)

	httpServer := &http.Server{
		Addr:              s.WsPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	s.lifecycleMu.Lock()
	if s.stopping {
		s.lifecycleMu.Unlock()
		return errors.New("websocket server is shut down")
	}
	if s.httpServer != nil {
		s.lifecycleMu.Unlock()
		return errors.New("websocket server already started")
	}
	s.httpServer = httpServer
	s.lifecycleMu.Unlock()

	log.Info("WebSocket 服务启动: addr=%s, path=/ws", s.WsPort)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.lifecycleMu.Lock()
		if !s.stopping {
			s.httpServer = nil
		}
		s.lifecycleMu.Unlock()
		log.Error("WebSocket 服务异常退出: addr=%s, err=%v", s.WsPort, err)
		return err
	}
	log.Info("WebSocket 服务已停止: addr=%s", s.WsPort)
	return nil
}

func (s *WsServer) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	s.lifecycleMu.Lock()
	if s.stopping {
		s.lifecycleMu.Unlock()
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}
	s.connectionsWG.Add(1)
	s.lifecycleMu.Unlock()

	writer := &websocketResponseWriter{ResponseWriter: w}
	var tracked *Client
	pumpsStarted := false
	defer func() {
		if pumpsStarted {
			return
		}
		if tracked != nil {
			tracked.closeNow()
			s.finishConnection(tracked)
		} else {
			if writer.transport != nil {
				_ = writer.transport.Close()
			}
			s.connectionsWG.Done()
		}
	}()

	conn, err := websocket.Accept(writer, r, &s.acceptOptions)
	if err != nil {
		log.Warn("WebSocket 握手失败, remote=%s, origin=%s, err=%v",
			r.RemoteAddr, r.Header.Get("Origin"), err)
		return
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	client := &Client{
		Conn:      conn,
		Server:    s,
		transport: writer.transport,
		ctx:       connCtx,
		cancel:    connCancel,
		closeDone: make(chan struct{}),
		SendCh:    make(chan []byte, 512),
		pending:   make(map[string]chan *RPCMessage),
		HandlerCh: make(chan RPCMessage, s.handlerQueue),
	}

	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		client.IP = host
		if p, convErr := strconv.Atoi(port); convErr == nil {
			client.Port = p
		} else {
			client.Port = -1
		}
	} else {
		client.IP = r.RemoteAddr
		client.Port = -1
	}

	tracked = client
	// 升级和关停可能交叉，关停开始后立即放弃刚升级的连接。
	s.lifecycleMu.Lock()
	if s.stopping {
		s.lifecycleMu.Unlock()
		client.forceClose()
		return
	}
	s.connections[client] = struct{}{}
	s.lifecycleMu.Unlock()

	conn.SetReadLimit(s.readLimit)
	if err := client.authenticate(authWait); err != nil {
		log.Warn("认证失败, IP: %s:%d, err: %v", client.IP, client.Port, err)
		return
	}

	pumpsStarted = true
	go client.runPumps(s.readLimit, newInboundLimiter(s.messageRate, s.byteRate, s.readLimit))
}

func (c *Client) runPumps(readLimit int64, limiter inboundLimiter) {
	s := c.Server
	defer s.finishConnection(c)
	s.connMgr.register(c)

	// 就绪回调执行期间仍然读取控制帧和检测断线，业务请求只入队。
	var pumps sync.WaitGroup
	readyDone := make(chan struct{})
	go func() {
		defer close(readyDone)
		defer c.finishSessionRegistration()
		if s.callClientReady(c) && c.registerReady() {
			pumps.Go(c.handlerPump)
			pumps.Go(c.writePump)
		} else {
			c.closeNow()
		}
	}()

	c.readPump(readLimit, limiter)
	<-readyDone
	<-c.closeDone
	c.removeFromPool()
	s.connMgr.unregister(c)
	s.callClientClose(c)
	pumps.Wait()
}

func (s *WsServer) callClientReady(c *Client) (ready bool) {
	if c.closing.Load() {
		return false
	}
	if s.onClientReady == nil {
		return true
	}
	defer func() {
		if r := recover(); r != nil {
			log.Error("onClientReady panic recovered: %v", r)
			c.CloseWithReason(websocket.StatusInternalError, "client setup failed")
		}
	}()
	s.onClientReady(c)
	return !c.closing.Load()
}

func (s *WsServer) callClientClose(c *Client) {
	if s.onClientClose == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Error("onClientClose panic recovered: %v", r)
		}
	}()
	s.onClientClose(c)
}

// authenticate 在连接进入 ConnManager 前完成认证。
func (c *Client) authenticate(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	msgType, data, err := c.Conn.Read(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// 未认证对端不会回应关闭帧，直接断开，避免为每条静默连接再等一次关闭握手。
			c.closeNow()
			return errors.New("auth timeout")
		}
		return fmt.Errorf("read auth message failed: %w", err)
	}
	if msgType != websocket.MessageText {
		_ = c.sendAuthResponse(false, "auth failed: text message required")
		c.CloseWithReason(websocket.StatusPolicyViolation, "auth failed: text message required")
		return errors.New("auth failed: text message required")
	}

	result, err := c.Server.authenticator(ctx, c, data)
	if err != nil {
		log.Warn("认证失败: %v", err)
		_ = c.sendAuthResponse(false, err.Error())
		c.CloseWithReason(websocket.StatusPolicyViolation, "auth failed: "+err.Error())
		return err
	}
	if result == nil || result.Type == "" || result.ID == "" {
		err := errors.New("invalid auth result")
		_ = c.sendAuthResponse(false, err.Error())
		c.CloseWithReason(websocket.StatusPolicyViolation, "auth failed: invalid auth result")
		return err
	}
	c.Type, c.ID = result.Type, result.ID
	c.codec = result.Codec
	c.beginSessionRegistration()

	if err := c.sendAuthResponse(true, ""); err != nil {
		c.finishSessionRegistration()
		return fmt.Errorf("send auth response failed: %w", err)
	}
	log.Info("客户端认证成功: 类型=%s, ID=%s, IP=%s:%d", c.Type, c.ID, c.IP, c.Port)
	return nil
}

func defaultAuthenticator(ctx context.Context, c *Client, data []byte) (*AuthResult, error) {
	var authMessage AuthMessage
	if err := json.Unmarshal(data, &authMessage); err != nil {
		log.Error("认证消息解析失败: %+v", err)
		return nil, fmt.Errorf("invalid format")
	}

	if authMessage.Type == "" || authMessage.ID == "" {
		log.Warn("非法 ID 或类型: %s", string(data))
		return nil, errors.New("invalid type/id")
	}

	return &AuthResult{
		Type: authMessage.Type,
		ID:   authMessage.ID,
	}, nil
}

// 记录认证结果被接受的顺序，写认证响应和执行就绪回调可以并发进行。
func (c *Client) beginSessionRegistration() {
	s := c.Server
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	c.authID = c.ID
	order := s.sessionOrders[c.authID]
	if order == nil {
		order = &sessionOrder{}
		s.sessionOrders[c.authID] = order
	}
	order.lastAuthenticated++
	order.pending++
	c.authSequence = order.lastAuthenticated
}

func (c *Client) finishSessionRegistration() {
	s := c.Server
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	order := s.sessionOrders[c.authID]
	order.pending--
	if order.pending == 0 {
		delete(s.sessionOrders, c.authID)
	}
}

func (c *Client) registerReady() bool {
	s := c.Server
	s.lifecycleMu.Lock()
	if s.stopping {
		s.lifecycleMu.Unlock()
		return false
	}
	clientType, id := c.Type, c.ID

	s.clientsMu.Lock()
	if c.closing.Load() {
		s.clientsMu.Unlock()
		s.lifecycleMu.Unlock()
		return false
	}
	order := s.sessionOrders[c.authID]
	if c.authSequence < order.lastReady {
		kick := c.closing.CompareAndSwap(false, true)
		s.clientsMu.Unlock()
		s.lifecycleMu.Unlock()
		if kick {
			// 过期连接也接收顶号通知，避免它自动重连再顶掉新会话。
			go c.notifyKickedAndClose()
		}
		return false
	}
	order.lastReady = c.authSequence
	oldClient := s.clientIDMap[id]
	var kickOld bool
	if oldClient != nil && oldClient != c {
		s.removeFromTypePool(oldClient)
		kickOld = oldClient.closing.CompareAndSwap(false, true)
	}
	if s.clientPool[clientType] == nil {
		s.clientPool[clientType] = make(map[string]*Client)
	}
	s.clientPool[clientType][id] = c
	s.clientIDMap[id] = c
	s.clientsMu.Unlock()
	s.lifecycleMu.Unlock()

	if kickOld {
		log.Warn("客户端ID %s 重新登录，新连接 %s:%d 将顶替旧连接 %s:%d",
			id, c.IP, c.Port, oldClient.IP, oldClient.Port)
		go oldClient.notifyKickedAndClose()
	}
	return true
}

// sendAuthResponse 向客户端发送认证结果。
func (c *Client) sendAuthResponse(success bool, errMsg string) error {
	if c.closing.Load() {
		log.Error("sendAuthResponse: client already closed, cannot send auth response (success=%v, err=%s)", success, errMsg)
		return errors.New("client connection closed")
	}

	resp := AuthResponse{
		OK: success,
	}
	if !success {
		resp.Error = errMsg
	}
	b, err := json.Marshal(resp)
	if err != nil {
		log.Error("序列化认证响应失败: %v", err)
		return err
	}

	if err := c.writeText(b, 2*time.Second); err != nil {
		log.Warn("发送认证响应失败: %v", err)
		return err
	}
	return nil
}

func (c *Client) writeText(message []byte, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	return c.Conn.Write(ctx, websocket.MessageText, message)
}

// CloseWithReason 用指定的状态码关闭连接。
func (c *Client) CloseWithReason(code websocket.StatusCode, text string) {
	if c.closing.CompareAndSwap(false, true) {
		c.finishClose(code, text)
	}
}

func (c *Client) closeNow() {
	if c.closing.CompareAndSwap(false, true) {
		defer close(c.closeDone)
		defer c.cancel()
		_ = c.Conn.CloseNow()
	}
}

func (c *Client) finishClose(code websocket.StatusCode, text string) {
	// 取消正在读写的 context 会直接断开底层连接，关闭握手结束后再取消。
	defer close(c.closeDone)
	defer c.cancel()
	_ = c.Conn.Close(code, text)
}

func (c *Client) notifyKickedAndClose() {
	defer c.finishClose(websocket.StatusPolicyViolation, "replaced by new connection")
	defer func() {
		if r := recover(); r != nil {
			log.Error("发送顶号通知时发生 panic: client=%s, panic=%v", c.ID, r)
		}
	}()

	// 直接写出通知，避免被普通业务发送队列挡住。
	message, err := c.marshalRPCMessage(RPCMessage{Method: "session.kicked", Error: "kicked"})
	if err == nil {
		err = c.writeText(message, 2*time.Second)
	}
	if err != nil {
		log.Warn("发送顶号通知失败: client=%s, err=%v", c.ID, err)
	}
}

// removeFromTypePool 只移除仍指向该连接的条目。调用方须持有 clientsMu 写锁。
func (s *WsServer) removeFromTypePool(c *Client) {
	clients := s.clientPool[c.Type]
	if clients[c.ID] == c {
		delete(clients, c.ID)
		if len(clients) == 0 {
			delete(s.clientPool, c.Type)
		}
	}
}

func (c *Client) removeFromPool() {
	c.cleanPendingWithError("connection lost")

	s := c.Server
	s.clientsMu.Lock()
	if s.clientIDMap[c.ID] == c {
		delete(s.clientIDMap, c.ID)
	}
	s.removeFromTypePool(c)
	s.clientsMu.Unlock()
}

// cleanPendingWithError 清理所有未完成的 Call，并返回指定错误
func (c *Client) cleanPendingWithError(errMsg string) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for session, ch := range c.pending {
		select {
		case ch <- &RPCMessage{Session: session, Error: errMsg}:
		default:
			log.Warn("cleanPendingWithError: receiver gone for session %s", session)
		}
		delete(c.pending, session)
	}
}

func (c *Client) enqueueTextMessage(msg RPCMessage) {
	select {
	case c.HandlerCh <- msg:
	default:
		err := fmt.Errorf("handler queue full")
		log.Warn("drop request: %v, method=%s, session=%s, client=%s:%d",
			err, msg.Method, msg.Session, c.IP, c.Port)
		if msg.Session != "" && c.ctx.Err() == nil {
			c.meResponse(msg.Session, nil, err)
		}
	}
}

func (c *Client) handlerPump() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.HandlerCh:
			if c.closing.Load() {
				return
			}
			c.dispatchTextMessage(c.ctx, msg)
		}
	}
}

func (c *Client) dispatchTextMessage(ctx context.Context, msg RPCMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panic recovered: method=%s, panic=%v", msg.Method, r)
			if msg.Session != "" && ctx.Err() == nil {
				c.meResponse(msg.Session, nil, fmt.Errorf("handler panic: %v", r))
			}
		}
	}()

	rsp, err := c.handleTextMessage(msg)
	if ctx.Err() != nil {
		log.Warn("drop response for closed connection, method=%s, session=%s", msg.Method, msg.Session)
		return
	}
	if msg.Session != "" {
		c.meResponse(msg.Session, &rsp, err)
	}
}

func (c *Client) handleTextMessage(msg RPCMessage) (any, error) {
	s := c.Server
	s.handlersMu.RLock()
	methods, typeExists := s.handlers[c.Type]
	var handler Handler
	if typeExists {
		handler = methods[msg.Method]
	}
	defaultHandler := s.defaultHandler
	s.handlersMu.RUnlock()

	if handler != nil {
		return handler(c, msg)
	}
	if defaultHandler != nil {
		return defaultHandler(c, msg)
	}

	log.Error("method '%s' not found for type %s, data: %s", msg.Method, c.Type, msg.Data)
	return nil, fmt.Errorf("method '%s' not found for type %s", msg.Method, c.Type)
}

// RegisterHandler 注册特定客户端类型的特定方法
func (s *WsServer) RegisterHandler(clientType string, method string, handler func(clt *Client, data json.RawMessage) (any, error)) {
	s.handlersMu.Lock()
	defer s.handlersMu.Unlock()

	if _, ok := s.handlers[clientType]; !ok {
		s.handlers[clientType] = make(map[string]Handler)
	}
	s.handlers[clientType][method] = func(clt *Client, msg RPCMessage) (any, error) {
		return handler(clt, msg.Data)
	}
	log.Info("注册路由: [%s] -> %s", clientType, method)
}

func (s *WsServer) RegisterDefaultHandler(handler Handler) {
	s.handlersMu.Lock()
	defer s.handlersMu.Unlock()

	if handler == nil {
		s.defaultHandler = nil
		return
	}
	s.defaultHandler = handler
	log.Info("注册默认路由")
}

// GetClientByID 根据客户端ID获取Client指针（并发安全）
func (s *WsServer) GetClientByID(clientID string) (*Client, bool) {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	client, ok := s.clientIDMap[clientID]
	return client, ok
}

func CallT[T any](c *Client, method string, arg any) (*T, json.RawMessage, *Client, error) {
	b, err := c.Call(method, arg, 5*time.Second)
	if err != nil {
		log.Error("call error (ID:%s): %v", c.ID, err)
		return nil, nil, nil, err
	}

	var data T
	if err := json.Unmarshal(b, &data); err != nil {
		log.Error("json.Unmarshal error: %+v", err)
		return nil, nil, c, fmt.Errorf("unmarshal response failed: %w", err)
	}

	return &data, b, c, nil
}

func CallByID[T any](s *WsServer, clientID string, method string, arg any) (*T, json.RawMessage, *Client, error) {
	c, exists := s.GetClientByID(clientID)

	if !exists {
		return nil, nil, nil, fmt.Errorf("no client found for id: %s", clientID)
	}
	return CallT[T](c, method, arg)
}

func (s *WsServer) SendByID(clientID string, method string, arg any) error {
	c, exists := s.GetClientByID(clientID)

	if !exists {
		return fmt.Errorf("no client found for id: %s", clientID)
	}
	return c.Send(method, arg)
}

func (s *WsServer) HasClient(clientType string) bool {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	return len(s.clientPool[clientType]) != 0
}

// WalkClient 遍历指定类型的客户端
func (s *WsServer) WalkClient(clientType string, f func(clt *Client)) bool {
	s.clientsMu.RLock()
	typeMap, exists := s.clientPool[clientType]
	if !exists || len(typeMap) == 0 {
		s.clientsMu.RUnlock()
		return false
	}

	// 策略：为了不在锁内执行耗时的业务逻辑 f，先拷贝所有指针
	clients := make([]*Client, 0, len(typeMap))
	for _, clt := range typeMap {
		clients = append(clients, clt)
	}
	s.clientsMu.RUnlock() // 拷贝完立即释放锁

	// 在无锁状态下执行回调
	for _, clt := range clients {
		f(clt)
	}

	return true
}
