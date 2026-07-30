// Package connlease 使用 Redis 租约、Pub/Sub 踢线通知和 fencing epoch，保证同一
// session key 在多个服务节点间只有一个有效连接所有者。
//
// # 打开管理器
//
//	manager, err := connlease.Open(ctx, connlease.Config{
//		Addr:          "127.0.0.1:6379",
//		ServerAddr:    "gateway-01",
//		KeyPrefix:     "session:",
//		SessionTTL:    30 * time.Second,
//		RenewInterval: 10 * time.Second,
//	})
//	if err != nil {
//		return err
//	}
//	defer manager.Close()
//
// ServerAddr 必须能唯一标识当前服务节点。KeyPrefix 为空时使用 session:；
// SessionTTL 和 RenewInterval 未设置时分别为 30 秒和 10 秒，且续租间隔必须
// 小于租约 TTL。运行状态统一通过 github.com/chenyao-lib/go/log 输出。
//
// # 注册连接
//
//	handle, err := manager.Register(ctx, userID, func(reason string) {
//		_ = conn.Close(websocket.StatusPolicyViolation, reason)
//	})
//	if err != nil {
//		return err
//	}
//	defer manager.Unregister(context.Background(), handle)
//
// Register 会原子替换该 key 的 owner、生成唯一 connection ID、递增 epoch，并
// 通知旧连接下线。同一个 key 在同节点或其他节点再次注册时，旧 Handle 都会
// 失效，其 closeFn 每个 Handle 最多执行一次。
//
// 每次处理来自连接的业务消息前，应检查本地所有权：
//
//	if err := manager.Validate(handle); err != nil {
//		return connlease.ErrOwnershipLost
//	}
//
// Validate 只检查本地 active 状态，不会在消息热路径访问 Redis。Pub/Sub 通知
// 负责快速失效，后台续租负责处理漏通知和 Redis 所有权变化。Handle.Active
// 可用于快速观察，但需要错误语义时应使用 Validate。
//
// # 释放和 fencing
//
// 底层连接关闭时必须调用 Unregister；释放脚本会比较 connection ID，旧连接
// 不会误删新连接的 owner。Handle.Epoch 是单调递增 fencing token，若后端写入
// 也必须拒绝旧连接，可把 epoch 一并写入下游存储并仅接受更新值。
//
// Manager.Close 会停止订阅与续租，并尽力释放本进程仍持有的 owner。关闭后
// Register 返回 ErrManagerClosed；Validate 对已失效句柄返回 ErrOwnershipLost。
package connlease

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyao-lib/go/log"
	"github.com/redis/go-redis/v9"
)

const (
	defaultKeyPrefix     = "session:"
	defaultSessionTTL    = 30 * time.Second
	defaultRenewInterval = 10 * time.Second
	redisCallTimeout     = 2 * time.Second
)

var (
	// ErrOwnershipLost 表示当前连接已不是该 session key 的有效 owner。
	ErrOwnershipLost = errors.New("connection ownership lost")
	// ErrManagerClosed 表示会话管理器已经进入关闭流程。
	ErrManagerClosed = errors.New("session manager is closed")
)

// acquireScript 在一个 Redis 原子操作中替换 owner、递增 fencing epoch 并刷新租约。
// 返回旧 owner 是为了让新连接能够通过 PubSub 精确踢掉旧 connection_id。
var acquireScript = redis.NewScript(`
local old_server = redis.call("HGET", KEYS[1], "server_addr") or ""
local old_connection = redis.call("HGET", KEYS[1], "connection_id") or ""
local epoch = redis.call("INCR", KEYS[2])
redis.call("HSET", KEYS[1],
	"server_addr", ARGV[1],
	"connection_id", ARGV[2],
	"epoch", epoch)
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return {old_server, old_connection, tostring(epoch)}
`)

// renewScript 只允许当前 connection_id 续租，旧连接不能延长新连接的租约。
var renewScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "connection_id") == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// releaseScript 使用 compare-and-delete，防止旧连接关闭时误删新连接的 owner。
var releaseScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "connection_id") == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// Config 定义跨节点单连接会话所需的 Redis、节点标识和租约参数。
type Config struct {
	Addr          string
	Password      string
	DB            int
	KeyPrefix     string
	ServerAddr    string
	SessionTTL    time.Duration
	RenewInterval time.Duration
}

// Manager 通过 Redis owner、PubSub 和定时续租管理每个 session key 的全局唯一连接。
type Manager struct {
	client        *redis.Client
	pubsub        *redis.PubSub
	keyPrefix     string
	serverAddr    string
	sessionTTL    time.Duration
	renewInterval time.Duration

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        sync.WaitGroup

	mu             sync.RWMutex
	byConnectionID map[string]*Handle
}

// Handle 表示已向 Manager 注册的一条连接。调用方应保存该句柄，并在消息处理前
// 调用 Validate，在底层连接关闭时调用 Unregister。
type Handle struct {
	manager      *Manager
	key          string
	connectionID string
	epoch        int64
	close        func(reason string)
	// active 是消息热路径的本地判断依据，避免每条消息访问 Redis。
	// PubSub 踢出或续租失败时必须先置为 false，再关闭底层连接。
	active    atomic.Bool
	closeOnce sync.Once
}

// Key 返回注册该连接时使用的会话 key。
func (h *Handle) Key() string {
	if h == nil {
		return ""
	}
	return h.key
}

// ConnectionID 返回 Manager 为本次连接生成的唯一 ID。
func (h *Handle) ConnectionID() string {
	if h == nil {
		return ""
	}
	return h.connectionID
}

// Epoch 返回本次抢占获得的单调递增 fencing token。
func (h *Handle) Epoch() int64 {
	if h == nil {
		return 0
	}
	return h.epoch
}

// Active 返回该句柄当前是否仍持有本地有效状态。
func (h *Handle) Active() bool {
	return h != nil && h.active.Load()
}

type kickMessage struct {
	Key          string `json:"machine_id"` // 保留旧字段名，兼容滚动升级中的节点。
	ConnectionID string `json:"connection_id"`
}

type previousOwner struct {
	serverAddr   string
	connectionID string
}

// Open 创建会话管理器，并启动踢线订阅和租约续期协程。
func Open(ctx context.Context, cfg Config) (*Manager, error) {
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	cfg.ServerAddr = strings.TrimSpace(cfg.ServerAddr)
	if cfg.Addr == "" {
		return nil, errors.New("redis address is empty")
	}
	if cfg.ServerAddr == "" {
		return nil, errors.New("server address is empty")
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = defaultSessionTTL
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = defaultRenewInterval
	}
	if cfg.RenewInterval >= cfg.SessionTTL {
		return nil, errors.New("session renew interval must be shorter than session ttl")
	}
	cfg.KeyPrefix = normalizeKeyPrefix(cfg.KeyPrefix)

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  redisCallTimeout,
		WriteTimeout: redisCallTimeout,
		PoolSize:     10,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		log.Error("[CONNLEASE] Redis 连接失败: addr=%s, db=%d, err=%v", cfg.Addr, cfg.DB, err)
		return nil, fmt.Errorf("connect session redis: %w", err)
	}

	managerCtx, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		client:         client,
		keyPrefix:      cfg.KeyPrefix,
		serverAddr:     cfg.ServerAddr,
		sessionTTL:     cfg.SessionTTL,
		renewInterval:  cfg.RenewInterval,
		ctx:            managerCtx,
		cancel:         cancel,
		byConnectionID: make(map[string]*Handle),
	}
	manager.pubsub = client.Subscribe(ctx, manager.kickChannel())
	if _, err := manager.pubsub.Receive(ctx); err != nil {
		cancel()
		_ = manager.pubsub.Close()
		_ = client.Close()
		log.Error("[CONNLEASE] 踢线频道订阅失败: channel=%s, err=%v", manager.kickChannel(), err)
		return nil, fmt.Errorf("subscribe session kick channel: %w", err)
	}

	manager.wg.Add(2)
	go manager.listenKicks()
	go manager.renewConnections()
	log.Info(
		"[CONNLEASE] 管理器启动成功: addr=%s, db=%d, server=%s, prefix=%s, ttl=%s, renew=%s",
		cfg.Addr,
		cfg.DB,
		cfg.ServerAddr,
		cfg.KeyPrefix,
		cfg.SessionTTL,
		cfg.RenewInterval,
	)
	return manager, nil
}

// Register 原子抢占指定 key 的 owner；抢占成功后通知旧 connection_id 下线。
// closeFn 会在所有权丢失或续租失败时被调用，并且对每个 Handle 最多调用一次。
func (m *Manager) Register(
	ctx context.Context,
	key string,
	closeFn func(reason string),
) (*Handle, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, errors.New("session key is empty")
	}
	if closeFn == nil {
		return nil, errors.New("session close callback is nil")
	}
	select {
	case <-m.ctx.Done():
		return nil, ErrManagerClosed
	default:
	}

	connectionID, err := newConnectionID()
	if err != nil {
		return nil, fmt.Errorf("generate connection id: %w", err)
	}
	previous, epoch, err := m.acquire(ctx, key, connectionID)
	if err != nil {
		log.Error("[CONNLEASE] 获取连接所有权失败: key=%s, server=%s, err=%v", key, m.serverAddr, err)
		return nil, err
	}

	entry := &Handle{
		manager:      m,
		key:          key,
		connectionID: connectionID,
		epoch:        epoch,
		close:        closeFn,
	}
	entry.active.Store(true)
	m.mu.Lock()
	m.byConnectionID[connectionID] = entry
	m.mu.Unlock()

	if previous.connectionID != "" && previous.connectionID != connectionID {
		log.Warn(
			"[CONNLEASE] 新连接替换旧连接: key=%s, old_server=%s, old_connection=%s, new_server=%s, new_connection=%s",
			key,
			previous.serverAddr,
			previous.connectionID,
			m.serverAddr,
			connectionID,
		)
		if err := m.publishKick(ctx, key, previous.connectionID); err != nil {
			log.Error(
				"[CONNLEASE] 发布旧连接踢线消息失败: key=%s, old_server=%s, old_connection=%s, err=%v",
				key,
				previous.serverAddr,
				previous.connectionID,
				err,
			)
		}
	}
	log.Info(
		"[CONNLEASE] 已获取连接所有权: key=%s, server=%s, connection=%s, epoch=%d",
		key,
		m.serverAddr,
		connectionID,
		epoch,
	)
	return entry, nil
}

// Unregister 先从本地索引移除连接，再按 connection_id 条件释放 Redis owner。
func (m *Manager) Unregister(ctx context.Context, handle *Handle) {
	entry := m.removeConnection(handle)
	if entry == nil {
		return
	}
	entry.active.Store(false)
	if m.ctx.Err() != nil {
		log.Debug(
			"[CONNLEASE] 管理器已关闭，仅移除本地连接: key=%s, connection=%s",
			entry.key,
			entry.connectionID,
		)
		return
	}
	releaseCtx, cancel := withRedisTimeout(ctx)
	defer cancel()
	released, err := releaseScript.Run(
		releaseCtx,
		m.client,
		[]string{m.ownerKey(entry.key)},
		entry.connectionID,
	).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Error(
			"[CONNLEASE] 释放连接所有权失败: key=%s, connection=%s, err=%v",
			entry.key,
			entry.connectionID,
			err,
		)
		return
	}
	if released == 1 {
		log.Info(
			"[CONNLEASE] 已释放连接所有权: key=%s, connection=%s",
			entry.key,
			entry.connectionID,
		)
	} else {
		log.Debug(
			"[CONNLEASE] 跳过所有权释放，连接已不是当前 owner: key=%s, connection=%s",
			entry.key,
			entry.connectionID,
		)
	}
}

// Validate 只读取本地 active，不在消息热路径访问 Redis。
// 全局 owner 的变化由 PubSub 快速通知，并由定时续租作为漏消息后的兜底检测。
func (m *Manager) Validate(handle *Handle) error {
	if handle == nil || handle.manager != m || !handle.active.Load() {
		return ErrOwnershipLost
	}
	if current := m.connectionByID(handle.connectionID); current != handle {
		return ErrOwnershipLost
	}
	return nil
}

// Close 停止后台协程，并尽力释放仍由本进程持有的 owner。
func (m *Manager) Close() error {
	var closeErr error
	m.closeOnce.Do(func() {
		entries := m.connectionSnapshot()
		log.Info(
			"[CONNLEASE] 正在关闭管理器: server=%s, active_connections=%d",
			m.serverAddr,
			len(entries),
		)
		m.cancel()
		if m.pubsub != nil {
			if err := m.pubsub.Close(); err != nil {
				log.Warn("[CONNLEASE] 关闭踢线订阅失败: err=%v", err)
				closeErr = err
			}
		}
		m.wg.Wait()

		for _, entry := range entries {
			entry.active.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), redisCallTimeout)
			err := releaseScript.Run(
				ctx,
				m.client,
				[]string{m.ownerKey(entry.key)},
				entry.connectionID,
			).Err()
			cancel()
			if err != nil && !errors.Is(err, redis.Nil) {
				log.Error(
					"[CONNLEASE] 关闭时释放连接所有权失败: key=%s, connection=%s, err=%v",
					entry.key,
					entry.connectionID,
					err,
				)
				if closeErr == nil {
					closeErr = err
				}
			}
		}
		if err := m.client.Close(); err != nil {
			log.Warn("[CONNLEASE] 关闭 Redis 客户端失败: err=%v", err)
			if closeErr == nil {
				closeErr = err
			}
		}
		if closeErr != nil {
			log.Warn("[CONNLEASE] 管理器关闭完成但存在错误: server=%s, err=%v", m.serverAddr, closeErr)
		} else {
			log.Info("[CONNLEASE] 管理器已关闭: server=%s", m.serverAddr)
		}
	})
	return closeErr
}

func (m *Manager) acquire(
	ctx context.Context,
	key string,
	connectionID string,
) (previousOwner, int64, error) {
	acquireCtx, cancel := withRedisTimeout(ctx)
	defer cancel()
	result, err := acquireScript.Run(
		acquireCtx,
		m.client,
		[]string{m.ownerKey(key), m.epochKey(key)},
		m.serverAddr,
		connectionID,
		m.sessionTTL.Milliseconds(),
	).Slice()
	if err != nil {
		return previousOwner{}, 0, fmt.Errorf("acquire connection ownership: %w", err)
	}
	if len(result) != 3 {
		return previousOwner{}, 0, errors.New("invalid connection ownership response")
	}
	epoch, err := redisResultInt64(result[2])
	if err != nil {
		return previousOwner{}, 0, fmt.Errorf("parse connection epoch: %w", err)
	}
	return previousOwner{
		serverAddr:   redisResultString(result[0]),
		connectionID: redisResultString(result[1]),
	}, epoch, nil
}

func (m *Manager) publishKick(ctx context.Context, key, connectionID string) error {
	data, err := json.Marshal(kickMessage{Key: key, ConnectionID: connectionID})
	if err != nil {
		return err
	}
	publishCtx, cancel := withRedisTimeout(ctx)
	defer cancel()
	return m.client.Publish(publishCtx, m.kickChannel(), data).Err()
}

func (m *Manager) listenKicks() {
	defer m.wg.Done()
	channel := m.pubsub.Channel()
	for {
		select {
		case <-m.ctx.Done():
			return
		case message, ok := <-channel:
			if !ok {
				if m.ctx.Err() == nil {
					log.Warn("[CONNLEASE] 踢线订阅意外关闭: channel=%s", m.kickChannel())
				}
				return
			}
			var kick kickMessage
			if err := json.Unmarshal([]byte(message.Payload), &kick); err != nil {
				log.Warn("[CONNLEASE] 解析踢线消息失败: payload=%s, err=%v", message.Payload, err)
				continue
			}
			entry := m.connectionByID(kick.ConnectionID)
			if entry == nil || entry.key != kick.Key {
				continue
			}
			// connection_id 精确匹配，避免延迟到达的消息踢掉后来建立的新连接。
			m.closeConnection(entry, "replaced by connection on another server", nil)
		}
	}
}

func (m *Manager) renewConnections() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			for _, entry := range m.connectionSnapshot() {
				ctx, cancel := context.WithTimeout(m.ctx, redisCallTimeout)
				renewed, err := renewScript.Run(
					ctx,
					m.client,
					[]string{m.ownerKey(entry.key)},
					entry.connectionID,
					m.sessionTTL.Milliseconds(),
				).Int()
				cancel()
				if err != nil {
					// Redis 不可用时采用 fail-closed，避免无法确认 owner 的连接继续处理业务。
					m.closeConnection(entry, "connection ownership unavailable", err)
					continue
				}
				if renewed != 1 {
					m.closeConnection(entry, "connection replaced", nil)
				}
			}
		}
	}
}

func (m *Manager) closeConnection(entry *Handle, reason string, cause error) {
	// 必须先失效本地状态；即使 WebSocket 关闭需要时间，后续消息也会立即被拒绝。
	entry.active.Store(false)
	entry.closeOnce.Do(func() {
		if cause != nil {
			log.Error(
				"[CONNLEASE] 连接所有权检查失败，关闭连接: key=%s, connection=%s, reason=%s, err=%v",
				entry.key,
				entry.connectionID,
				reason,
				cause,
			)
		} else {
			log.Warn(
				"[CONNLEASE] 连接所有权已失效，关闭连接: key=%s, connection=%s, reason=%s",
				entry.key,
				entry.connectionID,
				reason,
			)
		}
		entry.close(reason)
	})
}

func (m *Manager) connectionByID(connectionID string) *Handle {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byConnectionID[connectionID]
}

func (m *Manager) connectionSnapshot() []*Handle {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Handle, 0, len(m.byConnectionID))
	for _, entry := range m.byConnectionID {
		result = append(result, entry)
	}
	return result
}

func (m *Manager) removeConnection(handle *Handle) *Handle {
	if handle == nil || handle.manager != m {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.byConnectionID[handle.connectionID]
	if entry != handle {
		return nil
	}
	delete(m.byConnectionID, entry.connectionID)
	return entry
}

func (m *Manager) ownerKey(key string) string {
	return m.keyPrefix + "{" + redisSlotKey(key) + "}:owner"
}

func (m *Manager) epochKey(key string) string {
	return m.keyPrefix + "{" + redisSlotKey(key) + "}:epoch"
}

func (m *Manager) kickChannel() string {
	return m.keyPrefix + "kick"
}

func normalizeKeyPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return defaultKeyPrefix
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return prefix
}

func redisSlotKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

func newConnectionID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func redisResultString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func redisResultInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		var result int64
		_, err := fmt.Sscan(typed, &result)
		return result, err
	case []byte:
		var result int64
		_, err := fmt.Sscan(string(typed), &result)
		return result, err
	default:
		return 0, fmt.Errorf("unsupported redis result type %T", value)
	}
}

func withRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, redisCallTimeout)
}
