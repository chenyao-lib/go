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
// SessionTTL 和 RenewInterval 未设置时分别为 30 秒和 10 秒。TTL 至少为 1 毫秒，
// 向下取整到毫秒，续租间隔必须小于取整后的 TTL。运行状态通过 github.com/chenyao-lib/go/log 输出。
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
// 负责快速失效，后台续租负责处理漏通知和 Redis 所有权变化。Redis 瞬时不可用时
// 单次续租失败不会立刻踢线，连续失败达到内部阈值后才关闭连接。Handle.Active
// 可用于快速观察，但需要错误语义时应使用 Validate。
//
// # 释放和 fencing
//
// 底层连接关闭时必须调用 Unregister；释放脚本会比较 connection ID，旧连接
// 不会误删新连接的 owner。Handle.Epoch 是单调递增 fencing token，若后端写入
// 也必须拒绝旧连接，可把 epoch 一并写入下游存储并仅接受更新值。
// Redis 为每个出现过的 session key 持久保留一个 epoch 计数键，以跨空闲期递增。
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
	ownerWorkers         = 8
	closeReleaseTimeout  = 5 * time.Second
	// maxRenewFailures 是续租连续失败阈值。
	// Redis 短暂抖动时单次失败不代表所有权丢失，连续多次失败才关闭连接，
	// 避免网络瞬断把所有在线连接全部踢掉。
	maxRenewFailures = 3
)

var (
	// ErrOwnershipLost 表示当前连接已不是该 session key 的有效 owner。
	ErrOwnershipLost = errors.New("connection ownership lost")
	// ErrManagerClosed 表示会话管理器已经进入关闭流程。
	ErrManagerClosed = errors.New("session manager is closed")
)

// Redis stores epochs as canonical decimal strings. Lua numbers cannot compare all int64 epochs exactly.
const restoreEpochFloorLua = `
local function is_epoch(value)
	return value == "0" or string.match(value, "^[1-9][0-9]*$")
end

local function restore_epoch_floor()
	local epoch = redis.call("HGET", KEYS[1], "epoch")
	if not epoch then
		return
	end
	local counter = redis.call("GET", KEYS[2])
	if not is_epoch(epoch) or (counter and not is_epoch(counter)) then
		error("invalid fencing epoch")
	end
	if not counter or #counter < #epoch or (#counter == #epoch and counter < epoch) then
		redis.call("SET", KEYS[2], epoch)
	end
end
`

// acquireScript 原子替换 owner、递增 fencing epoch 并刷新租约。
// 返回旧 owner 以便精确踢掉旧 connection_id。PERSIST 清除旧版本留下的 TTL。
var acquireScript = redis.NewScript(restoreEpochFloorLua + `
local old_server = redis.call("HGET", KEYS[1], "server_addr") or ""
local old_connection = redis.call("HGET", KEYS[1], "connection_id") or ""
restore_epoch_floor()
redis.call("INCR", KEYS[2])
redis.call("PERSIST", KEYS[2])
local epoch = redis.call("GET", KEYS[2])
redis.call("HSET", KEYS[1],
	"server_addr", ARGV[1],
	"connection_id", ARGV[2],
	"epoch", epoch)
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return {old_server, old_connection, epoch}
`)

// renewScript 只允许当前 connection_id 续租，旧连接不能延长新连接的租约。
// 保活用当前 owner 的任期校正计数器下限，并清除旧版本留下的 epoch TTL。
var renewScript = redis.NewScript(restoreEpochFloorLua + `
if redis.call("HGET", KEYS[1], "connection_id") == ARGV[1] then
	restore_epoch_floor()
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	redis.call("PERSIST", KEYS[2])
	return 1
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
	closeErr  error
	wg        sync.WaitGroup

	admissionMu sync.Mutex
	ownerOps    sync.WaitGroup // Close 等已接收的注册和注销完成后再接管清理

	mu             sync.RWMutex
	byConnectionID map[string]*Handle
}

const (
	leasePending = iota
	leaseActive
	leaseInactive
)

// Handle 表示已向 Manager 注册的一条连接。调用方应保存该句柄，并在消息处理前
// 调用 Validate，在底层连接关闭时调用 Unregister。
type Handle struct {
	manager      *Manager
	key          string
	connectionID string
	epoch        int64
	close        func(reason string)
	// state prevents a queued kick from being overwritten by a delayed acquire reply.
	state atomic.Int32
	// 使用本地单调时钟，续租未得到确认时也能按时停止放行。
	validUntil atomic.Pointer[time.Time]
	// renewFailures 记录连续续租失败次数，成功一次即清零。
	// 只有达到阈值才关闭连接，避免 Redis 瞬时抖动导致全量踢线。
	renewFailures atomic.Int32
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

// Active 返回该句柄是否仍有效且本地租约尚未到期。
func (h *Handle) Active() bool {
	if h == nil || h.state.Load() != leaseActive {
		return false
	}
	until := h.validUntil.Load()
	return until != nil && time.Now().Before(*until)
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
	cfg.SessionTTL = cfg.SessionTTL.Truncate(time.Millisecond)
	if cfg.SessionTTL == 0 {
		return nil, errors.New("session ttl must be at least 1ms")
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = defaultRenewInterval
	}
	if cfg.RenewInterval >= cfg.SessionTTL {
		return nil, errors.New("session renew interval must be shorter than session ttl")
	}
	cfg.KeyPrefix = normalizeKeyPrefix(cfg.KeyPrefix)

	client := redis.NewClient(&redis.Options{
		Addr:                  cfg.Addr,
		Password:              cfg.Password,
		DB:                    cfg.DB,
		DialTimeout:           3 * time.Second,
		ReadTimeout:           redisCallTimeout,
		WriteTimeout:          redisCallTimeout,
		PoolSize:              10,
		ContextTimeoutEnabled: true,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		log.Error("Redis 连接失败: addr=%s, db=%d, err=%v", cfg.Addr, cfg.DB, err)
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
	// 订阅必须挂在 managerCtx 上而不是调用方 ctx：Open 的 ctx 通常是启动期
	// 一次性 context，提前取消后订阅会静默断开，踢线通知从此失效。
	manager.pubsub = client.Subscribe(managerCtx, manager.kickChannel())
	if _, err := manager.pubsub.Receive(ctx); err != nil {
		cancel()
		_ = manager.pubsub.Close()
		_ = client.Close()
		log.Error("踢线频道订阅失败: channel=%s, err=%v", manager.kickChannel(), err)
		return nil, fmt.Errorf("subscribe session kick channel: %w", err)
	}

	manager.wg.Add(2)
	go manager.listenKicks()
	go manager.renewConnections()
	log.Info(
		"管理器启动成功: addr=%s, db=%d, server=%s, prefix=%s, ttl=%s, renew=%s",
		cfg.Addr,
		cfg.DB,
		cfg.ServerAddr,
		cfg.KeyPrefix,
		cfg.SessionTTL,
		cfg.RenewInterval,
	)
	return manager, nil
}

// Register 原子抢占指定 key 的 owner。确认抢占成功后，用独立的 Redis 超时通知旧 connection_id 下线。
// closeFn 会在所有权丢失或续租失败时异步调用，每个 Handle 最多调用一次。
// 回调可调用 Close；Close 不等待回调执行完成。
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
	if !m.beginOwnerOp() {
		return nil, ErrManagerClosed
	}
	defer m.ownerOps.Done()

	connectionID, err := newConnectionID()
	if err != nil {
		return nil, fmt.Errorf("generate connection id: %w", err)
	}
	// Reserve the ID before Redis can publish an ownership change for it.
	entry := &Handle{manager: m, key: key, connectionID: connectionID, close: closeFn}
	m.mu.Lock()
	m.byConnectionID[connectionID] = entry
	m.mu.Unlock()

	started := time.Now()
	previous, epoch, err := m.acquire(ctx, key, connectionID)
	if err != nil {
		m.removeConnection(entry)
		// Redis may have applied the script before the response timed out.
		// Use an independent deadline and only delete an owner from this attempt.
		if _, releaseErr := m.releaseOwner(context.Background(), key, connectionID); releaseErr != nil {
			log.Warn("清理未确认的连接租约失败: key=%s, connection=%s, err=%v", key, connectionID, releaseErr)
		}
		if m.ctx.Err() != nil {
			return nil, ErrManagerClosed
		}
		log.Error("获取连接所有权失败: key=%s, server=%s, err=%v", key, m.serverAddr, err)
		return nil, err
	}

	entry.epoch = epoch
	until := started.Add(m.sessionTTL)
	entry.validUntil.Store(&until)

	// Once Redis confirms the handoff, the previous owner has been replaced
	// even if the caller's request expires or this manager starts closing.
	if previous.connectionID != "" && previous.connectionID != connectionID {
		if old := m.connectionByID(previous.connectionID); old != nil && old.key == key {
			m.closeConnection(old, "connection replaced", nil)
		}
		log.Warn(
			"新连接替换旧连接: key=%s, old_server=%s, old_connection=%s, new_server=%s, new_connection=%s",
			key,
			previous.serverAddr,
			previous.connectionID,
			m.serverAddr,
			connectionID,
		)
		if err := m.publishKick(ctx, key, previous.connectionID); err != nil {
			log.Error(
				"发布旧连接踢线消息失败: key=%s, old_server=%s, old_connection=%s, err=%v",
				key,
				previous.serverAddr,
				previous.connectionID,
				err,
			)
		}
	}
	if m.ctx.Err() != nil {
		return nil, ErrManagerClosed // Close will release the indexed lease.
	}
	if !time.Now().Before(until) || !entry.state.CompareAndSwap(leasePending, leaseActive) {
		m.Unregister(ctx, entry)
		return nil, ErrOwnershipLost
	}
	log.Info(
		"已获取连接所有权: key=%s, server=%s, connection=%s, epoch=%d",
		key,
		m.serverAddr,
		connectionID,
		epoch,
	)
	if m.ctx.Err() != nil {
		return nil, ErrManagerClosed
	}
	if !entry.Active() {
		m.Unregister(ctx, entry)
		return nil, ErrOwnershipLost
	}
	return entry, nil
}

func (m *Manager) beginOwnerOp() bool {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	if m.ctx.Err() != nil {
		return false
	}
	m.ownerOps.Add(1)
	return true
}

// Unregister 先从本地索引移除连接，再按 connection_id 条件释放 Redis owner。
func (m *Manager) Unregister(ctx context.Context, handle *Handle) {
	if handle == nil || handle.manager != m || !m.beginOwnerOp() {
		return // 关闭期间保留索引，由 Close 统一释放
	}
	defer m.ownerOps.Done()
	entry := m.removeConnection(handle)
	if entry == nil {
		return
	}
	// 释放属于关闭清理，不能因为调用方 ctx 已取消（连接关闭路径上很常见）
	// 而直接失败，否则 owner 只能等 TTL 过期；只保留 value，超时单独控制。
	if ctx == nil {
		ctx = context.Background()
	}
	released, err := m.releaseOwner(context.WithoutCancel(ctx), entry.key, entry.connectionID)
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Error(
			"释放连接所有权失败: key=%s, connection=%s, err=%v",
			entry.key,
			entry.connectionID,
			err,
		)
		return
	}
	if released == 1 {
		log.Info(
			"已释放连接所有权: key=%s, connection=%s",
			entry.key,
			entry.connectionID,
		)
	} else {
		log.Debug(
			"跳过所有权释放，连接已不是当前 owner: key=%s, connection=%s",
			entry.key,
			entry.connectionID,
		)
	}
}

// Validate 检查本地有效状态和租约到期时间，不在消息热路径访问 Redis。
// 全局 owner 的变化由 PubSub 快速通知，并由定时续租作为漏消息后的兜底检测。
func (m *Manager) Validate(handle *Handle) error {
	if handle == nil || handle.manager != m || !handle.Active() || m.ctx.Err() != nil {
		return ErrOwnershipLost
	}
	if current := m.connectionByID(handle.connectionID); current != handle {
		return ErrOwnershipLost
	}
	return nil
}

// Close 停止后台协程，并用共同的 5 秒期限并行释放仍由本进程持有的 owner。
// 未成功释放的租约由 Redis 按 TTL 过期。
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.admissionMu.Lock()
		m.cancel()
		m.admissionMu.Unlock()
		m.ownerOps.Wait()

		entries := m.connectionSnapshot()
		log.Info(
			"正在关闭管理器: server=%s, active_connections=%d",
			m.serverAddr,
			len(entries),
		)
		// 与 resubscribeKicks 的替换互斥：这里取走并清空，避免关掉刚换上的新订阅。
		if ps := m.swapPubSub(nil); ps != nil {
			if err := ps.Close(); err != nil {
				log.Warn("关闭踢线订阅失败: err=%v", err)
				m.closeErr = err
			}
		}
		m.wg.Wait()

		for _, entry := range entries {
			m.removeConnection(entry)
		}
		ctx, cancel := context.WithTimeout(context.Background(), closeReleaseTimeout)
		err := m.releaseConnections(ctx, entries)
		cancel()
		if err != nil && m.closeErr == nil {
			m.closeErr = err
		}
		if err := m.client.Close(); err != nil {
			log.Warn("关闭 Redis 客户端失败: err=%v", err)
			if m.closeErr == nil {
				m.closeErr = err
			}
		}
		if m.closeErr != nil {
			log.Warn("管理器关闭完成但存在错误: server=%s, err=%v", m.serverAddr, m.closeErr)
		} else {
			log.Info("管理器已关闭: server=%s", m.serverAddr)
		}
	})
	return m.closeErr
}

func (m *Manager) releaseConnections(ctx context.Context, entries []*Handle) error {
	if len(entries) == 0 {
		return nil
	}
	var firstErr error
	var firstFailure sync.Once
	forEachConnection(ctx, entries, func(ctx context.Context, entry *Handle) {
		_, err := m.releaseOwner(ctx, entry.key, entry.connectionID)
		if err != nil && !errors.Is(err, redis.Nil) {
			log.Error("关闭时释放连接所有权失败: key=%s, connection=%s, err=%v", entry.key, entry.connectionID, err)
			firstFailure.Do(func() { firstErr = err })
		}
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	return firstErr
}

func forEachConnection(ctx context.Context, entries []*Handle, work func(context.Context, *Handle)) {
	jobs := make(chan *Handle)
	var workers sync.WaitGroup
	for range min(ownerWorkers, len(entries)) {
		workers.Go(func() {
			for entry := range jobs {
				if ctx.Err() != nil {
					return
				}
				work(ctx, entry)
			}
		})
	}
	defer workers.Wait()
	defer close(jobs)
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return
		case jobs <- entry:
		}
	}
}

func (m *Manager) releaseOwner(ctx context.Context, key, connectionID string) (int, error) {
	releaseCtx, cancel := withRedisTimeout(ctx)
	defer cancel()
	return releaseScript.Run(releaseCtx, m.client, []string{m.ownerKey(key)}, connectionID).Int()
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
	if ctx == nil {
		ctx = context.Background()
	}
	publishCtx, cancel := withRedisTimeout(context.WithoutCancel(ctx))
	defer cancel()
	return m.client.Publish(publishCtx, m.kickChannel(), data).Err()
}

func (m *Manager) listenKicks() {
	defer m.wg.Done()

	// FIX 2026-11-09 跨机多开: 旧实现在订阅 channel 关闭后直接 return。
	// Redis 重启、主从切换、网络抖动都会让订阅静默断开，而断开之后本进程再也
	// 收不到「被别的实例顶号」的通知 —— 老连接继续在线、新实例也认为自己持有
	// owner，双主就永久存在（renewConnections 只能自愈「自己丢了 owner」，
	// 发现不了「别人抢走了但没通知我」）。改为断线后退避重订阅。
	const resubscribeDelay = time.Second

	for m.ctx.Err() == nil {
		pubsub := m.currentPubSub()
		if pubsub == nil {
			return
		}
		channel := pubsub.Channel()
		dropped := false

		for !dropped {
			select {
			case <-m.ctx.Done():
				return
			case message, ok := <-channel:
				if !ok {
					dropped = true
					continue
				}
				var kick kickMessage
				if err := json.Unmarshal([]byte(message.Payload), &kick); err != nil {
					log.Warn("解析踢线消息失败: payload=%s, err=%v", message.Payload, err)
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

		if m.ctx.Err() != nil {
			return
		}
		log.Warn("踢线订阅意外关闭，%s 后重新订阅: channel=%s", resubscribeDelay, m.kickChannel())

		for {
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(resubscribeDelay):
			}
			if err := m.resubscribeKicks(); err != nil {
				log.Error("重新订阅踢线频道失败，将继续重试: channel=%s, err=%v", m.kickChannel(), err)
				continue
			}
			break
		}
	}
}

// currentPubSub 取当前踢线订阅，供 listenKicks 读取。
func (m *Manager) currentPubSub() *redis.PubSub {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pubsub
}

// swapPubSub 原子替换踢线订阅并返回旧实例，避免 Close 与重订阅互相踩。
func (m *Manager) swapPubSub(next *redis.PubSub) *redis.PubSub {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.pubsub
	m.pubsub = next
	return old
}

// resubscribeKicks 重新建立踢线订阅；成功后关闭旧订阅实例。
func (m *Manager) resubscribeKicks() error {
	ps := m.client.Subscribe(m.ctx, m.kickChannel())
	ctx, cancel := withRedisTimeout(m.ctx)
	defer cancel()
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return err
	}
	if old := m.swapPubSub(ps); old != nil {
		_ = old.Close()
	}
	log.Info("踢线订阅已重新建立: channel=%s", m.kickChannel())
	return nil
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
			forEachConnection(m.ctx, m.connectionSnapshot(), m.renewConnection)
		}
	}
}

func (m *Manager) renewConnection(ctx context.Context, entry *Handle) {
	// 快照可能包含本轮 tick 之前已被 Unregister 的连接，跳过。
	if entry.state.Load() != leaseActive {
		return
	}
	if !entry.Active() {
		m.closeConnection(entry, "connection ownership expired", nil)
		return
	}
	started := time.Now()
	callCtx, cancel := withRedisTimeout(ctx)
	renewed, err := renewScript.Run(
		callCtx,
		m.client,
		[]string{m.ownerKey(entry.key), m.epochKey(entry.key)},
		entry.connectionID,
		m.sessionTTL.Milliseconds(),
	).Int()
	cancel()
	if ctx.Err() != nil || entry.state.Load() != leaseActive {
		return
	}
	if err != nil {
		// 单次续租失败时保留现有租约，连续失败或本地租约到期时关闭连接。
		failures := entry.renewFailures.Add(1)
		log.Warn("连接续租失败: key=%s, connection=%s, failures=%d/%d, err=%v",
			entry.key, entry.connectionID, failures, maxRenewFailures, err)
		if failures >= maxRenewFailures || !entry.Active() {
			m.closeConnection(entry, "connection ownership unavailable", err)
		}
		return
	}
	entry.renewFailures.Store(0)
	if renewed != 1 {
		m.closeConnection(entry, "connection replaced", nil)
		return
	}
	until := started.Add(m.sessionTTL)
	entry.validUntil.Store(&until)
	if !entry.Active() {
		m.closeConnection(entry, "connection ownership expired", nil)
	}
}

func (m *Manager) closeConnection(entry *Handle, reason string, cause error) {
	// Pending registrations return ErrOwnershipLost. Only an active handle runs the close callback.
	if entry.state.Swap(leaseInactive) != leaseActive {
		return
	}
	if cause != nil {
		log.Error("连接所有权检查失败，关闭连接: key=%s, connection=%s, reason=%s, err=%v",
			entry.key, entry.connectionID, reason, cause)
	} else {
		log.Warn("连接所有权已失效，关闭连接: key=%s, connection=%s, reason=%s",
			entry.key, entry.connectionID, reason)
	}
	go func() {
		defer func() {
			if value := recover(); value != nil {
				log.Error("连接关闭回调 panic: key=%s, connection=%s, reason=%s, panic=%v",
					entry.key, entry.connectionID, reason, value)
			}
		}()
		entry.close(reason)
	}()
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
	entry.state.Store(leaseInactive)
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
