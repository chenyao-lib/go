package svc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyao-lib/go/log"
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
)

// ==================== 服务状态 ====================

type ServiceState int32

const (
	StateStopped  ServiceState = 0
	StateRunning  ServiceState = 1
	StateStopping ServiceState = 2
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

	rspCh chan *ServiceMsg `json:"-"`
}

// ==================== context 标记 ====================

type handlerCtxKey struct{}

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
	Request(ctx context.Context, method string, args any) (any, error)
	Post(ctx context.Context, method string, args any) error
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
	services: make(map[string]*Service),
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

	state       atomic.Int32
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
	log.Write(log.LevelInfo, "INFO", "%s| "+format, s.logArgs(args)...)
}

func (s *Service) Warn(format string, args ...any) {
	log.Write(log.LevelWarn, "WARN", "%s| "+format, s.logArgs(args)...)
}

func (s *Service) Error(format string, args ...any) {
	log.Write(log.LevelError, "ERROR", "%s| "+format, s.logArgs(args)...)
}

func (s *Service) Debug(format string, args ...any) {
	log.Write(log.LevelDebug, "DEBUG", "%s| "+format, s.logArgs(args)...)
}

func (s *Service) logArgs(args []any) []any {
	out := make([]any, 0, len(args)+1)
	out = append(out, s.name)
	out = append(out, args...)
	return out
}

func (s *Service) SetConfig(cfg ServiceConfig) {
	s.config = cfg
	s.msgCh = make(chan *ServiceMsg, s.config.QueueSize)
}

func (s *Service) SetHandlerTimeout(timeout time.Duration) {
	s.config.HandlerTimeout = timeout
}

func (s *Service) SetDefaultCallTimeout(timeout time.Duration) {
	s.config.CallTimeout = timeout
}

func (s *Service) SetMethodTimeout(method string, timeout time.Duration) {
	if s.methodTimeouts == nil {
		s.methodTimeouts = make(map[string]time.Duration)
	}
	s.methodTimeouts[method] = timeout
}

func (s *Service) SetMethodTimeouts(timeouts map[string]time.Duration) {
	if s.methodTimeouts == nil {
		s.methodTimeouts = make(map[string]time.Duration, len(timeouts))
	}
	for method, timeout := range timeouts {
		s.methodTimeouts[method] = timeout
	}
}

func (s *Service) SetCallTimeout(method string, timeout time.Duration) {
	if s.callTimeouts == nil {
		s.callTimeouts = make(map[string]time.Duration)
	}
	s.callTimeouts[method] = timeout
}

func (s *Service) SetCallTimeouts(timeouts map[string]time.Duration) {
	if s.callTimeouts == nil {
		s.callTimeouts = make(map[string]time.Duration, len(timeouts))
	}
	for method, timeout := range timeouts {
		s.callTimeouts[method] = timeout
	}
}

// ==================== 全局注册表 ====================

type registry struct {
	mu       sync.RWMutex
	services map[string]*Service
}

func (r *registry) register(name string, svc *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.services[name]; exists {
		return fmt.Errorf("service %q already registered", name)
	}
	r.services[name] = svc
	return nil
}

func (r *registry) unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services, name)
}

func (r *registry) get(name string) (*Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, ok := r.services[name]
	return svc, ok
}

// GetService 从全局注册表获取服务
func GetService(name string) (*Service, bool) {
	return globalReg.get(name)
}

// ==================== 生命周期 ====================

func (s *Service) Start() error {
	if !s.state.CompareAndSwap(int32(StateStopped), int32(StateRunning)) {
		return fmt.Errorf("service %s already started", s.name)
	}

	if err := globalReg.register(s.name, s); err != nil {
		s.state.Store(int32(StateStopped))
		return fmt.Errorf("register %s: %w", s.name, err)
	}

	s.wg.Add(1)
	go s.loop()

	if err := s.runInit(); err != nil {
		s.Stop()
		return fmt.Errorf("init %s: %w", s.name, err)
	}

	if s.config.MonitorInterval > 0 {
		s.wg.Add(1)
		go s.monitor()
	}

	s.Info("started (queue=%d)", s.config.QueueSize)
	return nil
}

func (s *Service) runInit() error {
	<-s.execToken
	defer func() { s.execToken <- struct{}{} }()

	ctx := context.WithValue(context.Background(), handlerCtxKey{}, true)

	s.Info("initializing...")

	if err := s.definition.Init(ctx); err != nil {
		s.Error("init failed: %v", err)
		return err
	}

	s.Info("init done")
	return nil
}

func (s *Service) Stop() {
	if !s.state.CompareAndSwap(int32(StateRunning), int32(StateStopping)) {
		return
	}

	globalReg.unregister(s.name)

	if s.config.MonitorInterval > 0 {
		close(s.monitorQuit)
	}

	close(s.quit)

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
				return
			}

			s.startHandler(msg)

		case <-s.quit:
			s.drainRemaining()
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
	go func(m *ServiceMsg) {
		defer s.wg.Done()
		defer func() { s.execToken <- struct{}{} }()
		s.handleOne(m)
	}(msg)
}

func (s *Service) handleOne(msg *ServiceMsg) {
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
		ctx, cancel := s.handlerContext(timeout)
		defer cancel()
		ctx = context.WithValue(ctx, handlerCtxKey{}, true)

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
	if timeout, ok := s.methodTimeouts[method]; ok {
		return timeout
	}
	return s.config.HandlerTimeout
}

func (s *Service) callTimeout(method string) time.Duration {
	if timeout, ok := s.callTimeouts[method]; ok {
		return timeout
	}
	return s.config.CallTimeout
}

func (s *Service) handlerContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), timeout)
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

func (s *Service) drainRemaining() {
	deadline := time.Now().Add(s.config.DrainTimeout)
	drained := 0

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			left := len(s.msgCh)
			if left > 0 {
				s.Warn("drain timeout: %d messages dropped", left)
			}
			return
		}

		select {
		case msg := <-s.msgCh:
			select {
			case <-s.execToken:
			case <-time.After(remaining):
				return
			}

			drained++
			s.startHandler(msg)
		default:
			s.Info("drained %d messages", drained)
			return
		}
	}
}

// ==================== 同步 RPC ====================

func (s *Service) Request(ctx context.Context, method string, args any) (any, error) {
	if s.state.Load() != int32(StateRunning) {
		return nil, ErrServiceNotRunning
	}
	if ctx != nil && ctx.Value(handlerCtxKey{}) != nil {
		return nil, ErrExternalOnly
	}
	return s.request(ctx, method, args)
}

func (s *Service) Call(ctx context.Context, targetService, method string, args any) (any, error) {
	if s.state.Load() != int32(StateRunning) {
		return nil, ErrCallerNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}

	target, ok := globalReg.get(targetService)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, targetService)
	}

	if target.state.Load() != int32(StateRunning) {
		return nil, fmt.Errorf("%w: %s (state=%d)", ErrServiceNotRunning, targetService, target.state.Load())
	}

	if ctx.Value(handlerCtxKey{}) != nil {
		s.releaseExecToken()
		defer s.acquireExecToken()
	}

	return target.request(ctx, method, args)
}

func (s *Service) releaseExecToken() {
	s.execToken <- struct{}{}
}

func (s *Service) acquireExecToken() {
	<-s.execToken
}

func (s *Service) request(parent context.Context, method string, args any) (any, error) {
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
		rspCh:   rspCh,
	}

	select {
	case s.msgCh <- msg:
	case <-s.quit:
		s.metrics.TotalCallTimeouts.Add(1)
		return nil, ErrServiceStopping
	case <-ctx.Done():
		s.metrics.TotalCallTimeouts.Add(1)
		return nil, ErrCallTimeout
	default:
		s.metrics.TotalSendFailures.Add(1)
		s.metrics.SendNBFullTotal.Add(1)
		return nil, ErrQueueFull
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

	if ctx.Value(handlerCtxKey{}) != nil {
		s.releaseExecToken()
		defer s.acquireExecToken()
	}

	select {
	case <-time.After(d):
	case <-s.quit:
	}
}

// ==================== 异步发送 ====================

func (s *Service) Post(ctx context.Context, method string, args any) error {
	if s.state.Load() != int32(StateRunning) {
		return ErrServiceNotRunning
	}
	if ctx != nil && ctx.Value(handlerCtxKey{}) != nil {
		return ErrExternalOnly
	}

	msg := &ServiceMsg{
		Method: method,
		Data:   args,
	}

	select {
	case s.msgCh <- msg:
		return nil
	default:
		s.metrics.TotalSendFailures.Add(1)
		s.metrics.SendNBFullTotal.Add(1)
		return ErrQueueFull
	}
}

func (s *Service) Send(targetService, method string, args any) error {
	if s.state.Load() != int32(StateRunning) {
		return ErrCallerNotRunning
	}

	target, ok := globalReg.get(targetService)
	if !ok {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, targetService)
	}

	if target.state.Load() != int32(StateRunning) {
		return fmt.Errorf("%w: %s (state=%d)", ErrServiceNotRunning, targetService, target.state.Load())
	}

	msg := &ServiceMsg{
		Method: method,
		Data:   args,
	}

	select {
	case target.msgCh <- msg:
		return nil
	default:
		s.metrics.TotalSendFailures.Add(1)
		target.metrics.SendNBFullTotal.Add(1)
		return ErrQueueFull
	}
}

// ==================== 内部工具 ====================

func (s *Service) nextSession() string {
	seed := s.sessionSeed.Add(1)
	return fmt.Sprintf("%s-%d-%d", s.name, time.Now().UnixNano(), seed)
}

// ==================== 状态查询 ====================

func (s *Service) State() ServiceState { return ServiceState(s.state.Load()) }
func (s *Service) IsRunning() bool     { return s.state.Load() == int32(StateRunning) }
func (s *Service) Name() string        { return s.name }
func (s *Service) QueueLen() int       { return len(s.msgCh) }
func (s *Service) QueueCap() int       { return cap(s.msgCh) }

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
	ql := len(s.msgCh)
	qc := cap(s.msgCh)
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
