// Package srvsvcbinding 把每个 srv.Client 连接绑定到一个独立的 svc.Instance，
// 并统一管理连接建立、消息分发、服务停止和中间件回滚。
//
// # 创建绑定
//
//	server := srv.NewServer(":8080")
//
//	binding := srvsvcbinding.NewClientServiceBinding(
//		server,
//		func(client *srv.Client) svc.Instance {
//			definition := NewConnectionDefinition(client)
//			return svc.NewService(definition)
//		},
//		srvsvcbinding.WithMiddleware(authzMiddleware),
//	)
//	_ = binding
//
//	_ = server.Start()
//
// 工厂应为每个连接返回新的 Service 实例，并确保 Name 在进程内唯一，例如包含
// client.ID。连接 ready 后 binding 按顺序执行中间件、创建并启动 Service；
// 连接关闭时先停止 Service，再按逆序执行已成功 ready 的中间件。
//
// NewClientServiceBinding 会接管 WsServer 的 OnClientReady、OnClientClose 和
// DefaultHandler。创建 binding 后不要再调用对应 setter 覆盖它们；需要额外
// 生命周期逻辑时应使用 Middleware。
//
// # 消息分发
//
// 带 Session 的客户端消息通过 Instance.RawCall 分发，Handler 返回值会作为 RPC
// 响应发回客户端；Session 为空的消息通过 Instance.RawSend 分发，不等待业务
// 返回值。客户端 method 必须与该连接 Service 的 Handler 名称一致。
//
// # 中间件
//
// Middleware 的执行顺序为：
//
//	OnClientReady:  m1 -> m2 -> Service.Start
//	BeforeMessage:  m1 -> m2 -> Service
//	OnClientClose:  Service.Stop -> m2 -> m1
//
// 任一 OnClientReady 失败会拒绝连接，并只回滚此前已成功的中间件。任一
// BeforeMessage 失败会终止本条消息的分发，但不会自动关闭连接。只需实现部分
// 生命周期时可使用 MiddlewareFuncs，未设置的函数会作为成功的空操作。
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
	log.Info("[SRVSVCBINDING] 连接服务绑定已创建: middlewares=%d", len(b.middlewares))
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
			log.Error(
				"[SRVSVCBINDING] 连接中间件初始化失败: client_type=%s, client_id=%s, err=%v",
				c.Type,
				c.ID,
				err,
			)
			b.states.Delete(c)
			b.stopConnection(ctx, state, c)
			c.CloseWithReason(1011, "prepare connection failed")
			return
		}
		state.readyMiddlewares = append(state.readyMiddlewares, middleware)
	}

	connSvc := b.svcFactory(c)
	if connSvc == nil {
		log.Error(
			"[SRVSVCBINDING] 连接服务创建失败，工厂返回 nil: client_type=%s, client_id=%s",
			c.Type,
			c.ID,
		)
		b.states.Delete(c)
		b.stopConnection(ctx, state, c)
		c.CloseWithReason(1011, "create connection service failed")
		return
	}
	if err := connSvc.Start(); err != nil {
		log.Error(
			"[SRVSVCBINDING] 连接服务启动失败: client_type=%s, client_id=%s, service=%s, err=%v",
			c.Type,
			c.ID,
			connSvc.Name(),
			err,
		)
		b.states.Delete(c)
		b.stopConnection(ctx, state, c)
		c.CloseWithReason(1011, "start connection service failed")
		return
	}
	state.service = connSvc
	log.Info(
		"[SRVSVCBINDING] 连接服务启动成功: client_type=%s, client_id=%s, service=%s",
		c.Type,
		c.ID,
		connSvc.Name(),
	)
}

// onClientClose 保证连接状态只取出一次，避免重复 close 导致重复清理。
func (b *ClientServiceBinding) onClientClose(c *srv.Client) {
	if value, ok := b.states.LoadAndDelete(c); ok {
		b.stopConnection(context.Background(), value.(*connectionState), c)
	} else {
		log.Debug("[SRVSVCBINDING] 连接关闭时未找到绑定状态: client_type=%s, client_id=%s", c.Type, c.ID)
	}
}

// handleMessage 先执行全部消息守卫，再根据 Session 区分 RawCall 和 RawSend。
// 中间件拒绝消息时，业务 Service 不会收到该消息。
func (b *ClientServiceBinding) handleMessage(c *srv.Client, msg srv.RPCMessage) (any, error) {
	value, ok := b.states.Load(c)
	if !ok {
		err := errors.New("connection service not found")
		log.Warn(
			"[SRVSVCBINDING] 消息分发失败，连接服务不存在: client_type=%s, client_id=%s, method=%s, session=%s",
			c.Type,
			c.ID,
			msg.Method,
			msg.Session,
		)
		return nil, err
	}
	state := value.(*connectionState)
	if state.service == nil {
		err := errors.New("connection service is not ready")
		log.Warn(
			"[SRVSVCBINDING] 消息分发失败，连接服务未就绪: client_type=%s, client_id=%s, method=%s, session=%s",
			c.Type,
			c.ID,
			msg.Method,
			msg.Session,
		)
		return nil, err
	}

	ctx := context.Background()
	for _, middleware := range state.readyMiddlewares {
		if err := middleware.BeforeMessage(ctx, c, msg); err != nil {
			log.Warn(
				"[SRVSVCBINDING] 消息被中间件拒绝: client_type=%s, client_id=%s, method=%s, session=%s, err=%v",
				c.Type,
				c.ID,
				msg.Method,
				msg.Session,
				err,
			)
			return nil, err
		}
	}

	if msg.Session != "" {
		return state.service.RawCall(ctx, msg.Method, msg.Data)
	}
	return nil, state.service.RawSend(ctx, msg.Method, msg.Data)
}

// stopConnection 先停止业务 Service，再逆序清理已经 ready 的中间件。
// 逆序与初始化顺序对称，既用于正常断线，也用于 ready/start 失败时回滚。
func (b *ClientServiceBinding) stopConnection(ctx context.Context, state *connectionState, c *srv.Client) {
	if state.service != nil {
		state.service.Stop()
		log.Info(
			"[SRVSVCBINDING] 连接服务已停止: client_type=%s, client_id=%s, service=%s",
			c.Type,
			c.ID,
			state.service.Name(),
		)
	}
	for idx := len(state.readyMiddlewares) - 1; idx >= 0; idx-- {
		state.readyMiddlewares[idx].OnClientClose(ctx, c)
	}
}
