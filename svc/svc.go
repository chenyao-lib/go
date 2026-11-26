// Package svc 提供进程内、按名称注册的串行消息服务。每个 Service 拥有独立队列，
// 支持同步调用、异步发送、处理超时、调用超时、优雅停止和指标快照。
//
// # 定义和启动服务
//
//	type EchoDefinition struct{}
//
//	func (EchoDefinition) Name() string { return "echo" }
//	func (EchoDefinition) Init(ctx context.Context) error { return nil }
//	func (EchoDefinition) Handlers() map[string]svc.Handler {
//		return map[string]svc.Handler{
//			"upper": func(ctx context.Context, args any) (any, error) {
//				text, ok := args.(string)
//				if !ok {
//					return nil, errors.New("args must be string")
//				}
//				return strings.ToUpper(text), nil
//			},
//		}
//	}
//
//	echo := svc.NewService(EchoDefinition{})
//	if err := echo.Start(); err != nil {
//		return err
//	}
//	defer echo.Stop()
//
// 同一进程内运行中的服务名称必须唯一。Start 会注册服务并启动队列，Stop 会先
// 从注册表移除服务，再在 DrainTimeout 内尽量处理剩余消息。Definition 还可实现
// ConfigProvider、DefaultHandlerTimeoutProvider、ServiceMethodTimeoutProvider
// 或 ServiceCallTimeoutProvider 提供配置。
//
// # 调用规则
//
// 外部 goroutine 直接进入某个服务时使用 RawCall 或 RawSend：
//
//	result, err := echo.RawCall(ctx, "upper", "hello")
//
// 在 Handler 内调用其他已注册服务时，使用当前 Service 的 Call 或 Send：
//
//	result, err := current.Call(ctx, "echo", "upper", "hello")
//
// RawCall/RawSend 不允许从 Service Handler 内调用；这样可以避免绕过执行令牌。
// Call 在等待目标服务时会临时释放当前服务的执行令牌，返回后再恢复，避免服务间
// 同步调用把当前队列永久阻塞。调用方仍应避免形成循环等待。
//
// Call 和 Sleep 必须由上下文所属的 Service 接收：在 A 的 Handler 里调用
// a.Call/a.Sleep。用其他服务的 ctx 调用、handler 返回后继续使用 ctx、或同一
// ctx 被并发用于 Call/Sleep 都会被拒绝：Call 返回 ErrTokenNotOwned/
// ErrHandlerFinished/ErrTokenAlreadyReleased，Sleep 记录错误日志并跳过令牌释放。
//
// RawCall、Call 和 RawSend 将传入的上下文传播给 Handler，并应用配置的处理时限。
// RawSend 入队后，上下文仍须在异步处理期间有效。
// RawCall、Call、RawSend 和 Send 都可能返回队列满、服务停止或超时错误；应使用
// errors.Is 判断 ErrQueueFull、ErrCallTimeout 等预定义错误。
//
// # 配置、日志和指标
//
// 队列大小等结构性配置应在 Start 前通过 Definition 的 ConfigProvider 或
// SetConfig 设置。SetHandlerTimeout/SetMethodTimeouts 控制处理时限，
// SetDefaultCallTimeout/SetCallTimeouts 控制等待响应的时限。
//
// Info、Warn、Error、Debug 会自动在日志正文前添加 serviceName|，并保留
// printf 风格的 go vet/gopls 参数检查。Snapshot 返回队列占用、调用超时、
// 处理超时、发送失败和 panic 等累计指标。
package svc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyao-lib/go/log"
	"github.com/outrigdev/goid"
)

// ==================== 预定义错误 ====================

var (
	ErrServiceNotFound   = errors.New("service not found")
	ErrServiceNotRunning = errors.New("service is not running")
	ErrMethodNotFound    = errors.New("method not found")
	ErrQueueFull         = errors.New("service message queue full")
	ErrServiceStopping   = errors.New("service is stopping")
	ErrCallTimeout       = errors.New("call timeout")
	ErrCallerNotRunning  = errors.New("caller service is not running")
	ErrExternalOnly      = errors.New("external entry cannot be used inside service handler")

	ErrTokenNotOwned        = errors.New("handler context belongs to another service")
	ErrHandlerFinished      = errors.New("handler context is already finished")
	ErrTokenAlreadyReleased = errors.New("handler context is already used by another Call/Sleep")
)

// ==================== 服务状态 ====================

type ServiceState int32

const (
	StateStopped  ServiceState = 0
	StateRunning  ServiceState = 1
	StateStopping ServiceState = 2
	StateStarting ServiceState = 3 // Init has not completed yet.
)

// ==================== 可配置选项 ====================

type ServiceConfig struct {
	QueueSize         int
	DrainTimeout      time.Duration
	MonitorInterval   time.Duration
	QueueAlertPercent int
	HandlerTimeout    time.Duration
	CallTimeout       time.Duration
}

func DefaultConfig() ServiceConfig {
	return ServiceConfig{
		QueueSize:         10240,
		DrainTimeout:      5 * time.Second,
		MonitorInterval:   10 * time.Second,
		QueueAlertPercent: 80,
		HandlerTimeout:    0,
		CallTimeout:       5 * time.Second,
	}
}

// ==================== 消息定义 ====================

type ServiceMsg struct {
	Session string `json:"session"`
	Method  string `json:"method"`
	Data    any    `json:"data"`
	Error   error  `json:"-"`
	IsRsp   bool   `json:"-"`

	ctx      context.Context
	rspCh    chan *ServiceMsg `json:"-"`
	initCall bool
}

// ==================== context 标记 ====================

type handlerCtxKey struct{}
type initScopeKey struct{}

type initScope struct {
	service *Service
	done    <-chan struct{}
}

// handlerExecState 按一次 handler 执行跟踪令牌的归还与临时让出。
type handlerExecState struct {
	owner        *Service
	initializing bool
	mu           sync.Mutex
	released     bool
	done         chan struct{}
}

func handlerStateFromContext(ctx context.Context) *handlerExecState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(handlerCtxKey{}).(*handlerExecState)
	return state
}

// ==================== Handler 与服务定义 ====================

type Handler func(ctx context.Context, args any) (any, error)

type Definition interface {
	Name() string
	Handlers() map[string]Handler
	Init(ctx context.Context) error
}

type Instance interface {
	Name() string
	Start() error
	Stop()
	RawCall(ctx context.Context, method string, args any) (any, error)
	RawSend(ctx context.Context, method string, args any) error
}

type ConfigProvider interface {
	Config() *ServiceConfig
}

type DefaultHandlerTimeoutProvider interface {
	DefaultHandlerTimeout() time.Duration
}

type ServiceMethodTimeoutProvider interface {
	MethodTimeouts() map[string]time.Duration
}

type ServiceCallTimeoutProvider interface {
	CallTimeouts() map[string]time.Duration
}

// ==================== 全局注册表（包内单例） ====================

var globalReg = &registry{
	services:     make(map[string]*Service),
	reservations: make(map[string]*NameReservation),
}

var initLookups sync.Map // goroutine ID -> *initLookup

type initLookup struct {
	service  *Service
	previous *initLookup
}

// ==================== 服务主体 ====================

type Service struct {
	definition     Definition
	name           string
	handlers       map[string]Handler
	methodTimeouts map[string]time.Duration
	callTimeouts   map[string]time.Duration
	config         ServiceConfig

	msgCh chan *ServiceMsg
	quit  chan struct{}
	wg    sync.WaitGroup

	// These goroutines contribute to wg and cannot wait for their own shutdown.
	executions sync.Map // goroutine IDs of this service's active handlers and Init

	// lifecycleMu 串行化 Start/Stop，保证 quit/monitorQuit 只关闭一次。
	// stopDone 非 nil 表示已开始停止，同时禁止再次 Start。
	lifecycleMu sync.Mutex
	stopDone    chan struct{}
	initCancel  context.CancelFunc // lifecycleMu 保护
	releaseName func()             // lifecycleMu 保护
	admissionMu sync.Mutex         // 将消息入队与停止接收新消息串行化

	// timeoutsMu 保护默认超时和方法级超时。
	timeoutsMu sync.RWMutex

	state       atomic.Int32
	initialized atomic.Bool
	sessionSeed atomic.Int64

	execToken chan struct{}

	metrics     ServiceMetrics
	monitorQuit chan struct{}
}

type ServiceMetrics struct {
	SendNBFullTotal   atomic.Uint64
	ProcessTotal      atomic.Uint64
	ProcessPanicTotal atomic.Uint64
	ProcessDurSumNs   atomic.Uint64

	TotalCalls           atomic.Uint64
	TotalCallTimeouts    atomic.Uint64
	TotalHandlerTimeouts atomic.Uint64
	TotalSendFailures    atomic.Uint64
	TotalPanics          atomic.Uint64
}

// ==================== 构造函数 ====================

func NewService(def Definition) *Service {
	cfg := DefaultConfig()
	if provider, ok := def.(ConfigProvider); ok {
		if provided := provider.Config(); provided != nil {
			cfg = *provided
		}
	}
	svc := &Service{
		definition:     def,
		name:           def.Name(),
		handlers:       def.Handlers(),
		methodTimeouts: make(map[string]time.Duration),
		callTimeouts:   make(map[string]time.Duration),
		config:         cfg,
		quit:           make(chan struct{}),
		monitorQuit:    make(chan struct{}),
		execToken:      make(chan struct{}, 1),
	}
	svc.state.Store(int32(StateStopped))
	svc.execToken <- struct{}{}

	if timeoutProvider, ok := def.(DefaultHandlerTimeoutProvider); ok {
		svc.config.HandlerTimeout = timeoutProvider.DefaultHandlerTimeout()
	}
	if timeoutProvider, ok := def.(ServiceMethodTimeoutProvider); ok {
		for method, timeout := range timeoutProvider.MethodTimeouts() {
			svc.methodTimeouts[method] = timeout
		}
	}
	if timeoutProvider, ok := def.(ServiceCallTimeoutProvider); ok {
		for method, timeout := range timeoutProvider.CallTimeouts() {
			svc.callTimeouts[method] = timeout
		}
	}

	svc.msgCh = make(chan *ServiceMsg, svc.config.QueueSize)
	return svc
}

func (s *Service) Info(format string, args ...any) {
	log.Write(log.LevelInfo, "INFO", false, "%s| %s", s.name, fmt.Sprintf(format, args...))
}

func (s *Service) Warn(format string, args ...any) {
	log.Write(log.LevelWarn, "WARN", false, "%s| %s", s.name, fmt.Sprintf(format, args...))
}

func (s *Service) Error(format string, args ...any) {
	log.Write(log.LevelError, "ERROR", false, "%s| %s", s.name, fmt.Sprintf(format, args...))
}

func (s *Service) Debug(format string, args ...any) {
	log.Write(log.LevelDebug, "DEBUG", false, "%s| %s", s.name, fmt.Sprintf(format, args...))
}

// SetConfig sets the startup configuration. Calls after startup begins are ignored.
// Use the timeout setters to adjust timeouts while a service is running.
func (s *Service) SetConfig(cfg ServiceConfig) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.stopDone != nil || s.State() != StateStopped {
		s.Warn("SetConfig ignored after startup began")
		return
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()
	s.config = cfg
	s.msgCh = make(chan *ServiceMsg, s.config.QueueSize)
}

func (s *Service) SetHandlerTimeout(timeout time.Duration) {
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()
	s.config.HandlerTimeout = timeout
}

func (s *Service) SetDefaultCallTimeout(timeout time.Duration) {
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()
	s.config.CallTimeout = timeout
}

func (s *Service) SetMethodTimeout(method string, timeout time.Duration) {
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()

	if s.methodTimeouts == nil {
		s.methodTimeouts = make(map[string]time.Duration)
	}
	s.methodTimeouts[method] = timeout
}

func (s *Service) SetMethodTimeouts(timeouts map[string]time.Duration) {
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()

	if s.methodTimeouts == nil {
		s.methodTimeouts = make(map[string]time.Duration, len(timeouts))
	}
	for method, timeout := range timeouts {
		s.methodTimeouts[method] = timeout
	}
}

func (s *Service) SetCallTimeout(method string, timeout time.Duration) {
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()

	if s.callTimeouts == nil {
		s.callTimeouts = make(map[string]time.Duration)
	}
	s.callTimeouts[method] = timeout
}

func (s *Service) SetCallTimeouts(timeouts map[string]time.Duration) {
	s.timeoutsMu.Lock()
	defer s.timeoutsMu.Unlock()

	if s.callTimeouts == nil {
		s.callTimeouts = make(map[string]time.Duration, len(timeouts))
	}
	for method, timeout := range timeouts {
		s.callTimeouts[method] = timeout
	}
}

// ==================== 全局注册表 ====================

type registry struct {
	mu           sync.RWMutex
	services     map[string]*Service
	reservations map[string]*NameReservation
}

// NameReservation lets one owner prepare multiple services with a shared name
// while published lookups continue to see the current service.
type NameReservation struct{ name string }

// ReserveName claims an unused service name until the reservation is released.
func ReserveName(name string) (*NameReservation, error) {
	globalReg.mu.Lock()
	defer globalReg.mu.Unlock()
	if globalReg.services[name] != nil || globalReg.reservations[name] != nil {
		return nil, fmt.Errorf("service %q already registered or reserved", name)
	}
	reservation := &NameReservation{name: name}
	globalReg.reservations[name] = reservation
	return reservation, nil
}

// PrepareContext initializes a service for the reserved name without publishing
// it. Once startup begins, release is called when the service stops. If startup
// is rejected before it begins, the caller still owns the reservation.
// release should return promptly and may use Release, but must not call Stop.
func (n *NameReservation) PrepareContext(ctx context.Context, service *Service, release func()) error {
	if service.name != n.name {
		return fmt.Errorf("service %q does not match reserved name %q", service.name, n.name)
	}
	globalReg.mu.RLock()
	active := globalReg.reservations[n.name] == n
	globalReg.mu.RUnlock()
	if !active {
		return fmt.Errorf("service name %q has a different reservation", n.name)
	}
	return service.startContext(ctx, false, release)
}

// Publish makes a running service with the reserved name visible by name.
// It replaces any service previously published by this reservation.
func (n *NameReservation) Publish(service *Service) error {
	return n.PublishWithCommit(service, nil)
}

// PublishWithCommit publishes the service and performs commit within the same
// lifecycle and registry locks. commit must return promptly and not call svc.
func (n *NameReservation) PublishWithCommit(service *Service, commit func()) error {
	if service.name != n.name {
		return fmt.Errorf("service %q does not match reserved name %q", service.name, n.name)
	}
	return service.publishReplacement(nil, n, commit)
}

// Release lets other services claim the name after the owner has stopped its services.
func (n *NameReservation) Release() {
	globalReg.mu.Lock()
	defer globalReg.mu.Unlock()
	if globalReg.reservations[n.name] == n {
		delete(globalReg.reservations, n.name)
	}
}

func (r *registry) register(name string, service, previous *Service, reservation *NameReservation, commit func()) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reservations[name] != reservation {
		return fmt.Errorf("service name %q has a different reservation", name)
	}
	if reservation == nil {
		if current := r.services[name]; current != nil && current != previous && current != service {
			return fmt.Errorf("service %q already registered", name)
		}
	}
	if commit != nil {
		commit()
	}
	r.services[name] = service
	return nil
}

func (r *registry) unregister(name string, service *Service) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.services[name] == service {
		delete(r.services, name)
	}
}

func (r *registry) get(name string) (*Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, ok := r.services[name]
	return svc, ok
}

// GetService 从全局注册表获取服务。Init 和它同步调用的初始化 handler
// 查找自身名称时，返回正在初始化的实例。
func GetService(name string) (*Service, bool) {
	if current, ok := initLookups.Load(goid.Get()); ok {
		for scope := current.(*initLookup); scope != nil; scope = scope.previous {
			if scope.service.name == name {
				return scope.service, true
			}
		}
	}
	return globalReg.get(name)
}

// GetServiceContext lets goroutines started during Init find the service itself
// while the initialization context remains active, even after Init returns.
// Other lookups follow GetService.
func GetServiceContext(ctx context.Context, name string) (*Service, bool) {
	if ctx != nil && ctx.Err() == nil {
		if scope, ok := ctx.Value(initScopeKey{}).(initScope); ok && scope.service.name == name {
			select {
			case <-scope.done:
			default:
				return scope.service, true
			}
		}
	}
	return GetService(name)
}

func (s *Service) enterInitLookup(id uint64) func() {
	current, _ := initLookups.Load(id)
	previous, _ := current.(*initLookup)
	initLookups.Store(id, &initLookup{service: s, previous: previous})
	return func() {
		if previous == nil {
			initLookups.Delete(id)
		} else {
			initLookups.Store(id, previous)
		}
	}
}

// ==================== 生命周期 ====================

func (s *Service) Start() error {
	return s.StartContext(context.Background())
}

// StartContext 启动服务，并将 ctx 传给 Init。Stop 会取消初始化上下文。
// Init 应在上下文取消时退出；ctx 取消不会停止已经启动成功的服务。
// Init 成功后才接收外部消息。Init 可以用传入的 ctx 同步调用自身的 handler。
func (s *Service) StartContext(ctx context.Context) error {
	return s.startContext(ctx, true, nil)
}

// PrepareContext starts the service and runs Init without publishing its name.
// The caller owns the prepared service and must Stop it when it is no longer needed.
func (s *Service) PrepareContext(ctx context.Context) error {
	return s.startContext(ctx, false, nil)
}

// PublishReplacement publishes a running service by name. If the name is already
// occupied, it may only replace previous. The caller manages the lifetime of previous.
func (s *Service) PublishReplacement(previous *Service) error {
	return s.publishReplacement(previous, nil, nil)
}

func (s *Service) publishReplacement(previous *Service, reservation *NameReservation, commit func()) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.State() != StateRunning {
		return ErrServiceNotRunning
	}
	return globalReg.register(s.name, s, previous, reservation, commit)
}

func (s *Service) startContext(ctx context.Context, publish bool, releaseName func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 串行化 Start/Stop：避免 Stop 关闭 quit 之后 Start 又把循环拉起来，
	// 也避免并发 Stop 重复 close(quit) 导致 panic。
	s.lifecycleMu.Lock()

	if s.stopDone != nil {
		s.lifecycleMu.Unlock()
		err := fmt.Errorf("service %s: Stop has been called, restart is not allowed", s.name)
		s.Warn("start rejected: %v", err)
		return err
	}

	if !s.state.CompareAndSwap(int32(StateStopped), int32(StateStarting)) {
		s.lifecycleMu.Unlock()
		err := fmt.Errorf("service %s already started", s.name)
		s.Warn("start rejected: %v", err)
		return err
	}

	if publish {
		if err := globalReg.register(s.name, s, nil, nil, nil); err != nil {
			s.state.Store(int32(StateStopped))
			s.lifecycleMu.Unlock()
			wrapped := fmt.Errorf("register %s: %w", s.name, err)
			s.Error("register failed: %v", wrapped)
			return wrapped
		}
	}

	initCtx, cancelInit := context.WithCancel(ctx)
	s.initCancel = cancelInit
	s.releaseName = releaseName
	s.wg.Add(2) // 消息循环和初始化
	go s.loop()
	s.lifecycleMu.Unlock()

	initErr := s.runInit(initCtx, cancelInit)
	s.lifecycleMu.Lock()
	if initErr == nil {
		initErr = initCtx.Err()
	}
	if initErr == nil {
		s.initialized.Store(true)
		s.state.Store(int32(StateRunning))
		if s.config.MonitorInterval > 0 {
			s.wg.Add(1)
			go s.monitor()
		}
	}
	s.wg.Done()
	s.lifecycleMu.Unlock()
	if initErr != nil {
		s.Stop()
		return fmt.Errorf("init %s: %w", s.name, initErr)
	}

	s.Info("started (queue=%d)", s.config.QueueSize)
	return nil
}

func (s *Service) runInit(initCtx context.Context, cancelInit context.CancelFunc) (err error) {
	select {
	case <-initCtx.Done():
		return initCtx.Err()
	case <-s.execToken:
	}
	id := goid.Get()
	s.executions.Store(id, struct{}{})
	defer s.executions.Delete(id)
	defer s.enterInitLookup(id)()
	state := &handlerExecState{owner: s, initializing: true, done: make(chan struct{})}
	defer s.finishHandler(state)
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("panic in Init: %v", value)
			s.Error("init failed: %v", err)
		}
		if err != nil {
			cancelInit()
		}
	}()
	ctx := context.WithValue(initCtx, initScopeKey{}, initScope{service: s, done: initCtx.Done()})
	ctx = context.WithValue(ctx, handlerCtxKey{}, state)
	if err := ctx.Err(); err != nil {
		return err
	}

	s.Info("initializing...")

	if err := s.definition.Init(ctx); err != nil {
		s.Error("init failed: %v", err)
		return err
	}

	s.Info("init done")
	return nil
}

// Stop closes admission and waits for the shared shutdown to complete or reach its drain limit.
// A direct call from this service's handler or Init starts shutdown and returns so that execution can exit.
func (s *Service) Stop() {
	done := s.beginStop()
	if done == nil {
		return
	}
	if _, executing := s.executions.Load(goid.Get()); !executing {
		<-done
	}
}

func (s *Service) beginStop() <-chan struct{} {
	s.lifecycleMu.Lock()

	if done := s.stopDone; done != nil {
		s.lifecycleMu.Unlock()
		return done
	}

	s.admissionMu.Lock()
	state := s.State()
	stopping := state == StateRunning || state == StateStarting
	if stopping {
		s.state.Store(int32(StateStopping))
	}
	s.admissionMu.Unlock()
	if !stopping {
		// 未启动或已处于停止流程，不关闭任何 channel。
		s.lifecycleMu.Unlock()
		return nil
	}

	stopDone := make(chan struct{})
	s.stopDone = stopDone
	if s.initCancel != nil {
		s.initCancel()
		s.initCancel = nil
	}

	globalReg.unregister(s.name, s)

	if s.config.MonitorInterval > 0 {
		close(s.monitorQuit)
	}

	close(s.quit)

	releaseName := s.releaseName
	s.releaseName = nil
	s.lifecycleMu.Unlock()
	defer func() { go s.waitForStop(stopDone) }()
	if releaseName != nil {
		releaseName()
	}
	return stopDone
}

func (s *Service) waitForStop(stopDone chan<- struct{}) {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.Info("stopped gracefully")
	case <-time.After(s.config.DrainTimeout + 1*time.Second):
		s.Warn("stop timeout, force exit")
	}

	s.state.Store(int32(StateStopped))
	close(stopDone)
}

// ==================== 消息循环 ====================

func (s *Service) loop() {
	defer s.wg.Done()

	for {
		select {
		case msg := <-s.msgCh:
			if msg.IsRsp && msg.rspCh != nil {
				s.deliverResponse(msg)
				continue
			}

			select {
			case <-s.execToken:
			case <-s.quit:
				// 已经取出的消息不在 msgCh 里，必须一并交给 drain，否则停止时会静默丢失。
				s.drainRemaining(msg)
				return
			}

			s.startHandler(msg)

		case <-s.quit:
			s.drainRemaining(nil)
			return
		}
	}
}

func (s *Service) deliverResponse(msg *ServiceMsg) {
	select {
	case msg.rspCh <- msg:
	default:
		s.Warn("response receiver gone for session %s", msg.Session)
	}
}

func (s *Service) startHandler(msg *ServiceMsg) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		id := goid.Get()
		s.executions.Store(id, struct{}{})
		defer s.executions.Delete(id)
		if msg.initCall {
			defer s.enterInitLookup(id)()
		}
		state := &handlerExecState{owner: s, initializing: msg.initCall, done: make(chan struct{})}
		defer s.finishHandler(state)
		s.handleOne(msg, state)
	}()
}

func (s *Service) handleOne(msg *ServiceMsg, state *handlerExecState) {
	if msg.ctx != nil && msg.ctx.Err() != nil {
		return
	}

	start := time.Now()
	var panicErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = fmt.Errorf("panic: %v", r)
				s.metrics.ProcessPanicTotal.Add(1)
				s.metrics.TotalPanics.Add(1)
				s.Error("panic in method %s: %v", msg.Method, r)
			}
		}()

		handler, ok := s.handlers[msg.Method]
		if !ok {
			panicErr = fmt.Errorf("%w: %s", ErrMethodNotFound, msg.Method)
			return
		}

		timeout := s.handlerTimeout(msg.Method)
		ctx, cancel := s.handlerContext(msg.ctx, timeout)
		defer cancel()
		ctx = context.WithValue(ctx, handlerCtxKey{}, state)

		res, err := handler(ctx, msg.Data)

		if msg.Session != "" && msg.rspCh != nil {
			s.sendResponse(msg, res, err)
		} else if err != nil && msg.Session == "" {
			s.Warn("async method %s error: %v", msg.Method, err)
		}
	}()

	dur := time.Since(start)
	s.metrics.ProcessTotal.Add(1)
	s.metrics.ProcessDurSumNs.Add(uint64(dur.Nanoseconds()))
	timeout := s.handlerTimeout(msg.Method)
	if timeout > 0 && dur > timeout {
		s.metrics.TotalHandlerTimeouts.Add(1)
		s.Warn("handler %s exceeded timeout: elapsed=%s timeout=%s", msg.Method, dur, timeout)
	}

	if panicErr != nil && msg.Session != "" && msg.rspCh != nil {
		s.sendResponse(msg, nil, panicErr)
	}
}

func (s *Service) handlerTimeout(method string) time.Duration {
	s.timeoutsMu.RLock()
	defer s.timeoutsMu.RUnlock()
	if timeout, ok := s.methodTimeouts[method]; ok {
		return timeout
	}
	return s.config.HandlerTimeout
}

func (s *Service) callTimeout(method string) time.Duration {
	s.timeoutsMu.RLock()
	defer s.timeoutsMu.RUnlock()
	if timeout, ok := s.callTimeouts[method]; ok {
		return timeout
	}
	return s.config.CallTimeout
}

func (s *Service) handlerContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func (s *Service) sendResponse(msg *ServiceMsg, data any, err error) {
	rsp := &ServiceMsg{
		Session: msg.Session,
		Data:    data,
		Error:   err,
		IsRsp:   true,
	}
	select {
	case msg.rspCh <- rsp:
	default:
		s.Warn("caller for session %s already gone", msg.Session)
	}
}

func (s *Service) drainRemaining(pending *ServiceMsg) {
	if !s.initialized.Load() {
		s.discardQueuedMessages()
		return
	}
	deadline := time.Now().Add(s.config.DrainTimeout)
	drained := 0
	msg := pending

	for {
		if msg == nil {
			select {
			case next := <-s.msgCh:
				msg = next
			default:
				s.Info("drained %d messages", drained)
				return
			}
		}

		if remaining := time.Until(deadline); remaining > 0 {
			select {
			case <-s.execToken:
				drained++
				s.startHandler(msg)
				msg = nil
				continue
			case <-time.After(remaining):
			}
		}
		s.Warn("drain timeout: %d messages dropped", 1+s.discardQueuedMessages())
		return
	}
}

func (s *Service) discardQueuedMessages() int {
	// Stop has closed admission and the service loop is the only receiver.
	count := len(s.msgCh)
	for range count {
		<-s.msgCh
	}
	return count
}

// tryEnqueue 只在 Stop 开始排空前接受消息，且在持锁期间不等待队列空间。
func (s *Service) tryEnqueue(msg *ServiceMsg) error {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	state := s.State()
	if state != StateRunning && !(state == StateStarting && msg.initCall) {
		return ErrServiceNotRunning
	}
	select {
	case s.msgCh <- msg:
		return nil
	default:
		return ErrQueueFull
	}
}

// ==================== 同步 RPC ====================

func (s *Service) RawCall(ctx context.Context, method string, args any) (any, error) {
	if s.state.Load() != int32(StateRunning) {
		return nil, ErrServiceNotRunning
	}
	if ctx != nil && ctx.Value(handlerCtxKey{}) != nil {
		return nil, ErrExternalOnly
	}
	return s.rawCall(ctx, method, args, false)
}

func (s *Service) Call(ctx context.Context, targetService, method string, args any) (any, error) {
	state := handlerStateFromContext(ctx)
	callerState := s.State()
	initializing := callerState == StateStarting && state != nil && state.owner == s && state.initializing
	if callerState != StateRunning && !initializing {
		return nil, ErrCallerNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}

	target, ok := s.resolveTarget(targetService)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, targetService)
	}

	initCall := initializing && target == s
	targetState := target.State()
	if targetState != StateRunning && !(targetState == StateStarting && initCall) {
		return nil, fmt.Errorf("%w: %s (state=%d)", ErrServiceNotRunning, targetService, targetState)
	}

	if state != nil {
		if err := s.claimHandlerToken(state); err != nil {
			return nil, err
		}
		defer s.releaseHandlerToken(state)
	}

	return target.rawCall(ctx, method, args, initCall)
}

// claimHandlerToken 校验上下文属于 s 且尚未被并发使用，然后临时释放执行令牌。
func (s *Service) claimHandlerToken(state *handlerExecState) error {
	if state.owner != s {
		owner := "<nil>"
		if state.owner != nil {
			owner = state.owner.name
		}
		return fmt.Errorf("%w: ctx owner is %q", ErrTokenNotOwned, owner)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	select {
	case <-state.done:
		return ErrHandlerFinished
	default:
	}
	if state.released {
		return ErrTokenAlreadyReleased
	}
	state.released = true
	s.execToken <- struct{}{}
	return nil
}

func (s *Service) releaseHandlerToken(state *handlerExecState) {
	select {
	case <-state.done:
		return
	case <-s.execToken:
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	select {
	case <-state.done:
		// Handler 在重新拿到令牌期间退出，令牌归还给服务队列。
		s.execToken <- struct{}{}
	default:
		state.released = false
	}
}

func (s *Service) finishHandler(state *handlerExecState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	close(state.done)
	if !state.released {
		s.execToken <- struct{}{}
	}
}

func (s *Service) rawCall(parent context.Context, method string, args any, initCall bool) (any, error) {
	session := s.nextSession()
	rspCh := make(chan *ServiceMsg, 1)

	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := s.callContext(parent, method)
	defer cancel()

	msg := &ServiceMsg{
		Session: session,
		Method:  method,
		Data:    args,
		// Handler 继承调用方的上下文，CallTimeout 仍只约束等响应的时间。
		ctx:      parent,
		rspCh:    rspCh,
		initCall: initCall,
	}

	if ctx.Err() != nil {
		s.metrics.TotalCallTimeouts.Add(1)
		return nil, ErrCallTimeout
	}
	switch err := s.tryEnqueue(msg); err {
	case ErrServiceNotRunning:
		s.metrics.TotalCallTimeouts.Add(1)
		return nil, ErrServiceStopping
	case ErrQueueFull:
		s.metrics.TotalSendFailures.Add(1)
		s.metrics.SendNBFullTotal.Add(1)
		return nil, err
	}

	s.metrics.TotalCalls.Add(1)

	select {
	case rsp := <-rspCh:
		if rsp.Error != nil {
			return nil, rsp.Error
		}
		return rsp.Data, nil
	case <-ctx.Done():
		s.metrics.TotalCallTimeouts.Add(1)
		return nil, ErrCallTimeout
	case <-s.quit:
		return nil, ErrServiceStopping
	}
}

func (s *Service) callContext(parent context.Context, method string) (context.Context, context.CancelFunc) {
	timeout := s.callTimeout(method)
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

// ==================== Sleep ====================

func (s *Service) Sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if state := handlerStateFromContext(ctx); state != nil {
		if err := s.claimHandlerToken(state); err != nil {
			s.Error("sleep rejected: %v", err)
		} else {
			defer s.releaseHandlerToken(state)
		}
	}

	select {
	case <-time.After(d):
	case <-ctx.Done():
	case <-s.quit:
	}
}

// ==================== 异步发送 ====================

func (s *Service) RawSend(ctx context.Context, method string, args any) error {
	if s.state.Load() != int32(StateRunning) {
		return ErrServiceNotRunning
	}
	if ctx != nil {
		if ctx.Value(handlerCtxKey{}) != nil {
			return ErrExternalOnly
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	msg := &ServiceMsg{
		Method: method,
		Data:   args,
		ctx:    ctx,
	}

	err := s.tryEnqueue(msg)
	if err == ErrQueueFull {
		s.metrics.TotalSendFailures.Add(1)
		s.metrics.SendNBFullTotal.Add(1)
	}
	return err
}

func (s *Service) Send(targetService, method string, args any) error {
	state := s.State()
	if state != StateRunning && state != StateStarting {
		return ErrCallerNotRunning
	}

	target, ok := s.resolveTarget(targetService)
	if !ok {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, targetService)
	}

	msg := &ServiceMsg{
		Method: method,
		Data:   args,
	}

	err := target.tryEnqueue(msg)
	switch err {
	case ErrServiceNotRunning:
		return fmt.Errorf("%w: %s (state=%d)", err, targetService, target.state.Load())
	case ErrQueueFull:
		s.metrics.TotalSendFailures.Add(1)
		target.metrics.SendNBFullTotal.Add(1)
	}
	return err
}

// ==================== 内部工具 ====================

func (s *Service) resolveTarget(name string) (*Service, bool) {
	if name == s.name {
		return s, true
	}
	return globalReg.get(name)
}

func (s *Service) nextSession() string {
	seed := s.sessionSeed.Add(1)
	return fmt.Sprintf("%s-%d-%d", s.name, time.Now().UnixNano(), seed)
}

// ==================== 状态查询 ====================

func (s *Service) State() ServiceState { return ServiceState(s.state.Load()) }
func (s *Service) IsRunning() bool     { return s.state.Load() == int32(StateRunning) }
func (s *Service) Name() string        { return s.name }
func (s *Service) QueueLen() int {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	return len(s.msgCh)
}

func (s *Service) QueueCap() int {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	return cap(s.msgCh)
}

// ==================== 指标快照 ====================

type MetricsSnapshot struct {
	QueueLen             int    `json:"queue_len"`
	QueueCap             int    `json:"queue_cap"`
	QueuePercent         int    `json:"queue_percent"`
	TotalCalls           uint64 `json:"total_calls"`
	TotalCallTimeouts    uint64 `json:"total_call_timeouts"`
	TotalHandlerTimeouts uint64 `json:"total_handler_timeouts"`
	TotalSendFailures    uint64 `json:"total_send_failures"`
	TotalPanics          uint64 `json:"total_panics"`
}

func (s *Service) Snapshot() MetricsSnapshot {
	s.admissionMu.Lock()
	ql := len(s.msgCh)
	qc := cap(s.msgCh)
	s.admissionMu.Unlock()
	pct := 0
	if qc > 0 {
		pct = ql * 100 / qc
	}
	return MetricsSnapshot{
		QueueLen:             ql,
		QueueCap:             qc,
		QueuePercent:         pct,
		TotalCalls:           s.metrics.TotalCalls.Load(),
		TotalCallTimeouts:    s.metrics.TotalCallTimeouts.Load(),
		TotalHandlerTimeouts: s.metrics.TotalHandlerTimeouts.Load(),
		TotalSendFailures:    s.metrics.TotalSendFailures.Load(),
		TotalPanics:          s.metrics.TotalPanics.Load(),
	}
}

// ==================== 监控 ====================

func (s *Service) monitor() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.MonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.monitorQuit:
			return
		case <-ticker.C:
			s.checkAndAlert()
		}
	}
}

func (s *Service) checkAndAlert() {
	ql := len(s.msgCh)
	qc := cap(s.msgCh)

	threshold := qc * s.config.QueueAlertPercent / 100
	if ql > threshold {
		pct := ql * 100 / qc
		s.Warn("queue pressure: %d/%d (%d%%)", ql, qc, pct)
	}
	if ql == qc {
		s.Error("queue FULL (%d), new sends will fail", qc)
	}

	nbFail := s.metrics.SendNBFullTotal.Swap(0)
	if nbFail > 0 {
		s.Warn("send failures (queue full): %d", nbFail)
	}

	procTotal := s.metrics.ProcessTotal.Swap(0)
	procDur := s.metrics.ProcessDurSumNs.Swap(0)
	panicCount := s.metrics.ProcessPanicTotal.Swap(0)

	if procTotal > 0 {
		avgUs := float64(procDur) / float64(procTotal) / 1000.0
		s.Debug("processed=%d avg=%.2fμs panics=%d", procTotal, avgUs, panicCount)
	}
}
