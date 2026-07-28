package srvsvcbinding

import (
	"context"
	"errors"
	"sync"

	"github.com/chenyao-lib/go/srv"
	"github.com/chenyao-lib/go/svc"

	"github.com/chenyao-lib/go/log"
)

// Middleware 在不侵入具体业务 Service 的情况下扩展连接生命周期。
//
// OnClientReady 在业务 Service 启动前按注册顺序执行；某个中间件返回错误时，
// binding 只回滚此前已经 ready 成功的中间件，并拒绝当前连接。
// OnClientClose 在业务 Service 停止后按注册顺序的逆序执行。
// BeforeMessage 在每条 RPC 分发前执行，任一中间件返回错误都会终止本次分发，
// 但 binding 不会自动关闭连接；是否关闭应由对应中间件根据错误语义决定。
type Middleware interface {
	OnClientReady(ctx context.Context, c *srv.Client) error
	OnClientClose(ctx context.Context, c *srv.Client)
	BeforeMessage(ctx context.Context, c *srv.Client, msg srv.RPCMessage) error
}

// MiddlewareFuncs 允许调用方只提供需要的生命周期函数，未设置的函数视为成功的空操作。
type MiddlewareFuncs struct {
	ClientReady  func(ctx context.Context, c *srv.Client) error
	ClientClose  func(ctx context.Context, c *srv.Client)
	MessageGuard func(ctx context.Context, c *srv.Client, msg srv.RPCMessage) error
}

// OnClientReady 调用 ClientReady；未设置时直接返回成功。
func (m MiddlewareFuncs) OnClientReady(ctx context.Context, c *srv.Client) error {
	if m.ClientReady == nil {
		return nil
	}
	return m.ClientReady(ctx, c)
}

// OnClientClose 调用 ClientClose；未设置时不执行操作。
func (m MiddlewareFuncs) OnClientClose(ctx context.Context, c *srv.Client) {
	if m.ClientClose != nil {
		m.ClientClose(ctx, c)
	}
}

// BeforeMessage 调用 MessageGuard；未设置时允许消息继续分发。
func (m MiddlewareFuncs) BeforeMessage(ctx context.Context, c *srv.Client, msg srv.RPCMessage) error {
	if m.MessageGuard == nil {
		return nil
	}
	return m.MessageGuard(ctx, c, msg)
}

// Option 配置 ClientServiceBinding。
type Option func(*ClientServiceBinding)

// WithMiddleware 按传入顺序注册连接中间件，nil 中间件会被忽略。
func WithMiddleware(middlewares ...Middleware) Option {
	return func(b *ClientServiceBinding) {
		for _, middleware := range middlewares {
			if middleware != nil {
				b.middlewares = append(b.middlewares, middleware)
			}
		}
	}
}

// connectionState 保存单个客户端已启动的 Service 和已完成 ready 的中间件。
// readyMiddlewares 只记录成功项，因此初始化中途失败时可以做精确回滚。
type connectionState struct {
	service          svc.Instance
	readyMiddlewares []Middleware
}

// ClientServiceBinding 将每个 WebSocket 客户端绑定到独立的业务 Service，
// 并在 Service 外层统一执行连接生命周期和消息守卫中间件。
type ClientServiceBinding struct {
	states      sync.Map
	svcFactory  func(c *srv.Client) svc.Instance
	middlewares []Middleware
}

// NewClientServiceBinding 创建 binding，并独占设置 WsServer 的 ready、close
// 和 default message handler。调用方不应再覆盖这三个 hook；额外逻辑应通过
// WithMiddleware 注册，避免会话清理或业务 Service 生命周期被绕过。
func NewClientServiceBinding(
	ws *srv.WsServer,
	svcFactory func(c *srv.Client) svc.Instance,
	opts ...Option,
) *ClientServiceBinding {
	b := &ClientServiceBinding{svcFactory: svcFactory}
	for _, opt := range opts {
		if opt != nil {
			opt(b)
		}
	}
	ws.SetOnClientReady(b.onClientReady)
	ws.SetOnClientClose(b.onClientClose)
	ws.RegisterDefaultHandler(b.handleMessage)
	return b
}

// onClientReady 先准备全部中间件，再创建并启动业务 Service。
// 任一步失败都会删除连接状态、逆序回滚已 ready 的中间件并关闭客户端。
func (b *ClientServiceBinding) onClientReady(c *srv.Client) {
	state := &connectionState{}
	b.states.Store(c, state)
	ctx := context.Background()

	for _, middleware := range b.middlewares {
		if err := middleware.OnClientReady(ctx, c); err != nil {
			log.Error("connection middleware ready failed: %v", err)
			b.states.Delete(c)
			b.stopConnection(ctx, state, c)
			c.CloseWithReason(1011, "prepare connection failed")
			return
		}
		state.readyMiddlewares = append(state.readyMiddlewares, middleware)
	}

	connSvc := b.svcFactory(c)
	if connSvc == nil {
		log.Error("create connection service failed: factory returned nil")
		b.states.Delete(c)
		b.stopConnection(ctx, state, c)
		c.CloseWithReason(1011, "create connection service failed")
		return
	}
	if err := connSvc.Start(); err != nil {
		log.Error("start connection service failed: %v", err)
		b.states.Delete(c)
		b.stopConnection(ctx, state, c)
		c.CloseWithReason(1011, "start connection service failed")
		return
	}
	state.service = connSvc
	log.Info("connection service started: %s", connSvc.Name())
}

// onClientClose 保证连接状态只取出一次，避免重复 close 导致重复清理。
func (b *ClientServiceBinding) onClientClose(c *srv.Client) {
	if value, ok := b.states.LoadAndDelete(c); ok {
		b.stopConnection(context.Background(), value.(*connectionState), c)
	}
}

// handleMessage 先执行全部消息守卫，再根据 Session 区分 Request 和 Post。
// 中间件拒绝消息时，业务 Service 不会收到该消息。
func (b *ClientServiceBinding) handleMessage(c *srv.Client, msg srv.RPCMessage) (any, error) {
	value, ok := b.states.Load(c)
	if !ok {
		return nil, errors.New("connection service not found")
	}
	state := value.(*connectionState)
	if state.service == nil {
		return nil, errors.New("connection service is not ready")
	}

	ctx := context.Background()
	for _, middleware := range state.readyMiddlewares {
		if err := middleware.BeforeMessage(ctx, c, msg); err != nil {
			return nil, err
		}
	}

	if msg.Session != "" {
		return state.service.Request(ctx, msg.Method, msg.Data)
	}
	return nil, state.service.Post(ctx, msg.Method, msg.Data)
}

// stopConnection 先停止业务 Service，再逆序清理已经 ready 的中间件。
// 逆序与初始化顺序对称，既用于正常断线，也用于 ready/start 失败时回滚。
func (b *ClientServiceBinding) stopConnection(ctx context.Context, state *connectionState, c *srv.Client) {
	if state.service != nil {
		state.service.Stop()
		log.Info("connection service stopped: %s", state.service.Name())
	}
	for idx := len(state.readyMiddlewares) - 1; idx >= 0; idx-- {
		state.readyMiddlewares[idx].OnClientClose(ctx, c)
	}
}
