package srv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	writeWait           = 10 * time.Second
	authWait            = 10 * time.Second
	pongWait            = 10 * time.Second
	pingPeriod          = (pongWait * 9) / 10
	defaultHandlerQueue = 1024
)

// WsServer 代表一个独立的 WebSocket 服务实例
type WsServer struct {
	WsPort  string
	connMgr *ConnManager

	// 状态存储（原全局变量移动到此处）
	clientPool map[string]map[string]*Client
	poolMu     sync.RWMutex

	clientIDMap map[string]*Client
	idMapMu     sync.RWMutex

	handlers       map[string]map[string]Handler
	defaultHandler Handler
	handlersMu     sync.RWMutex

	httpServer *http.Server

	acceptOptions websocket.AcceptOptions
	handlerQueue  int
	authenticator Authenticator
	onClientReady ClientHook
	onClientClose ClientHook
}

type AuthResult struct {
	Type  string
	ID    string
	Codec payloadcodec.Codec
}

type Authenticator func(ctx context.Context, c *Client, data []byte) (*AuthResult, error)
type ClientHook func(c *Client)

// SetOriginPatterns 配置允许跨域访问 WebSocket 的 Origin 白名单。
// 不配置时默认允许所有 Origin，以保持 NewServer 的开箱即用行为。
func (s *WsServer) SetOriginPatterns(patterns ...string) {
	s.acceptOptions.OriginPatterns = append([]string(nil), patterns...)
	s.acceptOptions.InsecureSkipVerify = false
}

func (s *WsServer) SetHandlerQueueSize(n int) {
	if n > 0 {
		s.handlerQueue = n
	}
}

func (s *WsServer) SetAuthenticator(authenticator Authenticator) {
	if authenticator != nil {
		s.authenticator = authenticator
	}
}

func (s *WsServer) SetOnClientReady(fn ClientHook) {
	s.onClientReady = fn
}

func (s *WsServer) SetOnClientClose(fn ClientHook) {
	s.onClientClose = fn
}

// --- 核心结构 ---

type ConnManager struct {
	// 直接用 *websocket.Conn 映射其发送通道
	Clients    map[*websocket.Conn]*Client
	Broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
	Lock       sync.RWMutex
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
	Conn   *websocket.Conn
	Server *WsServer // 反向引用所属的 Server 实例
	ctx    context.Context
	cancel context.CancelFunc
	IP     string
	Port   int
	SendCh chan []byte
	Type   string //客户端类型
	ID     string //客户端ID，全局唯一
	codec  payloadcodec.Codec

	pending     map[string]chan *RPCMessage
	pendingMu   sync.Mutex
	sessionSeed int64
	closed      atomic.Bool // 标记连接是否已关闭（SendCh 已 close）
	sendMu      sync.RWMutex
	HandlerCh   chan RPCMessage
}

// --- 逻辑实现 ---

func (m *ConnManager) Start() {
	for {
		select {
		case client := <-m.Register:
			m.Lock.Lock()
			m.Clients[client.Conn] = client
			count := len(m.Clients)
			m.Lock.Unlock()
			log.Info("新连接接入, IP：%s:%d，当前总连接数: %d", client.IP, client.Port, count)

		case client := <-m.Unregister:
			m.Lock.Lock()
			if _, ok := m.Clients[client.Conn]; ok {
				delete(m.Clients, client.Conn)
				client.sendMu.Lock()
				client.closed.Store(true) // 先标记
				close(client.SendCh)
				client.sendMu.Unlock()
				count := len(m.Clients)
				log.Info("连接断开, IP: %s:%d，剩余连接数: %d", client.IP, client.Port, count)
			}
			m.Lock.Unlock()

		case message := <-m.Broadcast:
			m.Lock.RLock()
			for _, client := range m.Clients {
				select {
				case client.SendCh <- message:
				default:
					// 某个连接写缓冲区满了，忽略或处理
				}
			}
			m.Lock.RUnlock()
		}
	}
}

// 外部调用的 Send 函数保持不变...
func (c *Client) Send(method string, arg any) error {
	if c.closed.Load() {
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
	if c.closed.Load() {
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
	if c.closed.Load() {
		return nil, errors.New("client connection closed")
	}
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

	c.sendMu.RLock()
	if c.closed.Load() {
		c.sendMu.RUnlock()
		return nil, errors.New("client connection closed")
	}
	select {
	case c.SendCh <- b:
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
	if c.closed.Load() {
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
	if c.closed.Load() {
		c.sendMu.RUnlock()
		log.Warn("meResponse: client already closed, drop response for session %s", session)
		return
	}
	select {
	case c.SendCh <- b:
	case <-time.After(writeWait):
		log.Warn("meResponse: send timeout, drop response for session %s", session)
	}
	c.sendMu.RUnlock()
}

func (c *Client) nextSession() string {
	return strconv.FormatInt(atomic.AddInt64(&c.sessionSeed, 1), 10)
}

// --- 读写泵 (Pumps) ---

func (c *Client) readPump() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("readPump panic recovered: %v, client %s:%d", r, c.IP, c.Port)
		}
		c.cancel()
		c.removeFromPool()
		c.Server.connMgr.Unregister <- c
		c.Conn.CloseNow()
		c.Server.callClientClose(c)
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

		switch msgType {
		case websocket.MessageText:
			var msg RPCMessage
			err := json.Unmarshal(data, &msg)
			if err != nil {
				log.Info("收到非法消息: %s", string(data))
				continue
			}
			if err := c.decodeRPCMessage(&msg); err != nil {
				log.Warn("客户端消息解密失败: client=%s, method=%s, session=%s, err=%v",
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
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		if r := recover(); r != nil {
			log.Error("writePump panic recovered: %v, client %s:%d", r, c.IP, c.Port)
		}
		ticker.Stop()
		c.Conn.CloseNow()
	}()

	for {
		select {
		case <-c.ctx.Done():
			_ = c.Conn.Close(websocket.StatusNormalClosure, "")
			return
		case message, ok := <-c.SendCh:
			if !ok {
				_ = c.Conn.Close(websocket.StatusNormalClosure, "")
				return
			}
			ctx, cancel := context.WithTimeout(c.ctx, writeWait)
			err := c.Conn.Write(ctx, websocket.MessageText, message)
			cancel()
			if err != nil {
				return
			}

		case <-ticker.C:
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
		WsPort:      wsPort,
		clientPool:  make(map[string]map[string]*Client),
		clientIDMap: make(map[string]*Client),
		handlers:    make(map[string]map[string]Handler),
		acceptOptions: websocket.AcceptOptions{
			InsecureSkipVerify: true,
		},
		handlerQueue:  defaultHandlerQueue,
		authenticator: defaultAuthenticator,
	}
	// 直接初始化结构体
	mgr := &ConnManager{
		Clients:    make(map[*websocket.Conn]*Client),
		Broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
	}
	s.connMgr = mgr
	return s
}

func (s *WsServer) Start() error {
	mgr := s.connMgr
	go mgr.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &s.acceptOptions)
		if err != nil {
			log.Warn("WebSocket 握手失败, remote=%s, origin=%s, err=%v",
				r.RemoteAddr, r.Header.Get("Origin"), err)
			return
		}

		connCtx, connCancel := context.WithCancel(context.Background())
		client := &Client{
			Conn:        conn,
			Server:      s,
			ctx:         connCtx,
			cancel:      connCancel,
			SendCh:      make(chan []byte, 512),
			pending:     make(map[string]chan *RPCMessage),
			sessionSeed: 0,
			Type:        "",
			HandlerCh:   make(chan RPCMessage, s.handlerQueue),
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

		conn.SetReadLimit(512 * 1024)
		if err := client.authenticate(authWait); err != nil {
			log.Warn("认证失败, IP: %s:%d, err: %v", client.IP, client.Port, err)
			connCancel()
			conn.CloseNow()
			return
		}

		mgr.Register <- client
		s.callClientReady(client)
		go client.handlerPump()
		go client.writePump()
		go client.readPump()
	})

	s.httpServer = &http.Server{
		Addr:              s.WsPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Info("WebSocket Server 运行在 %s", s.WsPort)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("%+v", err)
		return err
	}
	return nil
}

// Shutdown 优雅关闭服务
func (s *WsServer) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	log.Info("开始优雅关闭 WebSocket 服务器...")
	// 停止接受新连接
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Error("httpServer.Shutdown 错误: %v", err)
		return err
	}
	log.Info("HTTP 层已关闭，等待所有 WebSocket 连接自然断开...")
	// 注意：还需要主动关闭所有客户端连接，让它们在 writePump/readPump 中退出
	s.closeAllClients()
	log.Info("所有客户端连接已关闭")
	return nil
}

// closeAllClients 遍历所有客户端并关闭连接
func (s *WsServer) closeAllClients() {
	s.idMapMu.RLock()
	clients := make([]*Client, 0, len(s.clientIDMap))
	for _, clt := range s.clientIDMap {
		clients = append(clients, clt)
	}
	s.idMapMu.RUnlock()

	for _, clt := range clients {
		clt.CloseWithReason(websocket.StatusGoingAway, "server shutdown")
	}
}

func (s *WsServer) callClientReady(c *Client) {
	if s.onClientReady == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Error("onClientReady panic recovered: %v", r)
		}
	}()
	s.onClientReady(c)
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
			c.CloseWithReason(websocket.StatusPolicyViolation, "auth timeout")
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
	c.codec = result.Codec

	if err := c.registerAuthenticated(result.Type, result.ID); err != nil {
		_ = c.sendAuthResponse(false, err.Error())
		c.CloseWithReason(websocket.StatusPolicyViolation, "auth failed: "+err.Error())
		return err
	}

	log.Info("客户端认证成功: 类型=%s, ID=%s, IP=%s:%d", result.Type, result.ID, c.IP, c.Port)

	if err := c.sendAuthResponse(true, ""); err != nil {
		c.removeFromPool()
		return fmt.Errorf("send auth response failed: %w", err)
	}

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

func (c *Client) registerAuthenticated(clientType string, id string) error {
	s := c.Server
	var oldClient *Client

	s.idMapMu.Lock()
	if current, exists := s.clientIDMap[id]; exists && current != c {
		oldClient = current
		log.Warn("客户端ID %s 重新登录，新连接 %s:%d 将顶替旧连接 %s:%d",
			id, c.IP, c.Port, current.IP, current.Port)
	}
	s.clientIDMap[id] = c
	s.idMapMu.Unlock()

	c.addToPool(clientType, id)

	if oldClient != nil {
		oldClient.CloseWithReason(websocket.StatusPolicyViolation, "replaced by new connection")
	}
	return nil
}

// sendAuthResponse 向客户端发送认证结果。
func (c *Client) sendAuthResponse(success bool, errMsg string) error {
	if c.closed.Load() {
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

	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	if err := c.Conn.Write(ctx, websocket.MessageText, b); err != nil {
		log.Warn("发送认证响应失败: %v", err)
		return err
	}
	return nil
}

func (c *Client) CloseWithReason(code websocket.StatusCode, text string) {
	c.cancel()
	_ = c.Conn.Close(code, text)
}

// 添加到对应类型的数组
func (c *Client) addToPool(clientType string, ID string) {
	c.Type = clientType
	c.ID = ID
	s := c.Server
	s.poolMu.Lock()
	defer s.poolMu.Unlock()

	if _, ok := s.clientPool[clientType]; !ok {
		s.clientPool[clientType] = make(map[string]*Client)
	}
	s.clientPool[clientType][ID] = c
}

// 从数组中移除（在 defer 中调用）
func (c *Client) removeFromPool() {
	// 连接已断开，立即让所有未完成的 Call 失败
	c.cleanPendingWithError("connection lost")

	if c.Type == "" {
		return
	}
	s := c.Server
	s.idMapMu.Lock()
	// 只有当 Map 里存的那个 Client 指针还是“我”自己时，我才去删它
	if current, exists := s.clientIDMap[c.ID]; exists && current == c {
		delete(s.clientIDMap, c.ID)
		log.Info("客户端ID %s 正常下线，清理完成", c.ID)
	} else {
		// 如果 current != c，说明 map 已经被新启动的连接覆盖了
		log.Info("客户端ID %s 已被新实例替换，跳过 Map 清理", c.ID)
	}
	s.idMapMu.Unlock()

	s.poolMu.Lock()
	defer s.poolMu.Unlock()

	if clients, ok := s.clientPool[c.Type]; ok {
		// 关键点：依然要校验指针，防止误删新重连的实例
		if current, exists := clients[c.ID]; exists && current == c {
			delete(clients, c.ID)
			log.Info("客户端已从池中移除: ID %s", c.ID)
		}
	}
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
	s.idMapMu.RLock()
	defer s.idMapMu.RUnlock()
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
	s.idMapMu.RLock()
	c, exists := s.clientIDMap[clientID]
	s.idMapMu.RUnlock()

	if !exists {
		return nil, nil, nil, fmt.Errorf("no client found for id: %s", clientID)
	}
	return CallT[T](c, method, arg)
}

func (s *WsServer) SendByID(clientID string, method string, arg any) error {
	s.idMapMu.RLock()
	c, exists := s.clientIDMap[clientID]
	s.idMapMu.RUnlock()

	if !exists {
		return fmt.Errorf("no client found for id: %s", clientID)
	}
	return c.Send(method, arg)
}

func (s *WsServer) HasClient(clientType string) bool {
	// 1. 加读锁，保证在获取切片时数据不会被其他协程修改
	s.poolMu.RLock()
	defer s.poolMu.RUnlock()
	clients, exists := s.clientPool[clientType]
	// 如果该类型不存在，或者切片为空，直接解锁并返回
	if !exists || len(clients) == 0 {
		return false
	}
	return true
}

// WalkClient 遍历指定类型的客户端
func (s *WsServer) WalkClient(clientType string, f func(clt *Client)) bool {
	s.poolMu.RLock()
	typeMap, exists := s.clientPool[clientType]
	if !exists || len(typeMap) == 0 {
		s.poolMu.RUnlock()
		return false
	}

	// 策略：为了不在锁内执行耗时的业务逻辑 f，先拷贝所有指针
	clients := make([]*Client, 0, len(typeMap))
	for _, clt := range typeMap {
		clients = append(clients, clt)
	}
	s.poolMu.RUnlock() // 拷贝完立即释放锁

	// 在无锁状态下执行回调
	for _, clt := range clients {
		f(clt)
	}

	return true
}
