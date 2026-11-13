// Package leader 基于 Redis 租约实现主节点选举，Go 与 Node.js 等其它语言共用
// 同一套 key 布局和 Lua 脚本，可以在同一个 group 内互相竞争、互相接管。
//
// # 用法
//
//	election, err := leader.Open(ctx, leader.Config{
//		Addr:     "127.0.0.1:6379", // host:port，不带 redis:// 前缀
//		Password: "",               // 没有密码留空
//		DB:       0,                // 业务使用的逻辑库编号
//		Group:    "billing",
//		Owner:    "billing-01",
//		OnChange: func(ev leader.Event) {
//			if ev.IsLeader {
//				startPrimaryWork()
//			} else {
//				stopPrimaryWork()
//			}
//		},
//	})
//	if err != nil {
//		return err
//	}
//	defer election.Close()
//
// Addr 只接受 host:port 单地址，连接池由库自己创建，暂不支持 Sentinel/Cluster。
// DB 要与业务读写使用同一个库，否则 Lua 里读不到 leader:{group}:epoch，无法做
// token 校验。
//
// Group 和 KeyPrefix 都相同的进程（不论实现语言）竞争同一个主节点位置；Owner
// 必须唯一标识一个进程，重复会让两个进程反复互抢。TTL 默认 10 秒，
// RenewInterval 和 RetryInterval 默认 TTL/3，续租间隔必须小于 TTL。
//
// 消息热路径只用 IsLeader 做纯内存判断，不要每次都访问 Redis：
//
//	if election.IsLeader() {
//		handleAsPrimary(msg)
//	}
//
// # 行为
//
// 后台协程在未当选时每 RetryInterval 抢占一次，当选后每 RenewInterval 续租一次。
// Redis 短暂不可用不会立刻让位：本地租约（最后一次成功续租的发起时间 + TTL）
// 未到期前继续担任主节点，避免网络抖动引起主从反复切换；本地租约到期仍未续租
// 成功则自动降级。Redis 侧租约被其它节点接管时立即降级。IsLeader 同时校验本地
// 租约是否到期，续租调用卡在网络上时也不会在到期后继续返回 true；OnChange 的
// 降级通知在该次调用返回后才发出。
//
// 降级后 Epoch 归零。Epoch 是 fencing token：每次新当选递增，续租期间保持不变。
// 校验责任在下游：主节点往下游写入时必须携带 Epoch，下游持久记录已见的最大值并
// 拒绝小于等于该值的写入，否则旧主节点（GC 暂停后复活、网络分区恢复）的延迟写入
// 无法被拦截。下游不做校验时，选主只能用于容忍偶尔双主的效率场景，例如避免多个
// 实例重复执行清理任务，不能用于对正确性有严格要求的写入。
//
// Redis 主从异步复制下，故障切换可能同时丢失 owner 和 epoch。为保证 token 不回退，
// 新当选时任期取 max(INCR, 当前毫秒)，写回 epoch key 后再使用；切换后新任期仍是
// 一个大于切换前的大数（除非节点时钟倒走）。Epoch 因此是毫秒量级的令牌，不是
// 从 1 开始的小计数。
//
// 进程退出前调用 Close：仍持有租约时会尽力释放，其它节点可以立即接管，不必等
// TTL 过期。Resign 会主动让位并永久停止本次选举，需要重新参与选举必须重新 Open。
// OnChange 可能在后台协程或调用 Resign/Close 的协程中执行，可能并发，回调必须
// 自行同步；不要在回调里调用 Close（它会等待后台协程退出），需要让位时用 Resign。
//
// # 与其它语言互通
//
// 其它语言按下面的 key 布局执行同样的 Lua 脚本即可参与选举。{group} 同时作为
// Redis Cluster 的 hash tag，保证两个 key 落在同一个 slot 上，脚本才能原子执行。
//
//	leader key: <KeyPrefix>{<group>}:owner
//	  Hash: owner=持有者 ID, epoch=任期, acquired_at=当选时间(ms), expires_at=租约到期时间(ms)
//	epoch key : <KeyPrefix>{<group>}:epoch
//	  String，不设过期时间，每次新当选按 max(INCR, now_ms) 推进，保证任期单调递增。
//
// 抢占或续租，KEYS = [leader key, epoch key]，ARGV = [owner, ttl ms, now ms]，
// 返回 {1, epoch, owner} 表示当选，{0, epoch, holder} 表示被他人持有：
//
//	local owner = redis.call("HGET", KEYS[1], "owner")
//	if owner and owner ~= ARGV[1] then
//		return {0, redis.call("HGET", KEYS[1], "epoch") or "0", owner}
//	end
//	local now = tonumber(ARGV[3])
//	local function next_epoch()
//		local counter = redis.call("INCR", KEYS[2])
//		if counter < now then
//			counter = now
//			redis.call("SET", KEYS[2], string.format("%.0f", counter))
//		end
//		return string.format("%.0f", counter)
//	end
//	local epoch
//	if owner == ARGV[1] then
//		epoch = redis.call("HGET", KEYS[1], "epoch")
//		if not epoch then
//			epoch = next_epoch()
//		end
//	else
//		epoch = next_epoch()
//		redis.call("HSET", KEYS[1], "acquired_at", ARGV[3])
//	end
//	redis.call("HSET", KEYS[1], "owner", ARGV[1], "epoch", epoch,
//		"expires_at", tostring(tonumber(ARGV[3]) + tonumber(ARGV[2])))
//	redis.call("PEXPIRE", KEYS[1], ARGV[2])
//	return {1, epoch, ARGV[1]}
//
// 主动让位，KEYS = [leader key]，ARGV = [owner]，返回 1 表示释放成功，0 表示
// 已经不是持有者：
//
//	if redis.call("HGET", KEYS[1], "owner") == ARGV[1] then
//		redis.call("DEL", KEYS[1])
//		return 1
//	end
//	return 0
//
// Node.js（ioredis）示例：
//
//	const [status, epoch, holder] = await redis.eval(
//		acquireScript, 2, ownerKey, epochKey, owner, String(ttlMs), String(Date.now()))
//	if (Number(status) === 1) { /* 当选或续租成功 */ } else { /* holder 是当前主节点 */ }
//
// now 用于观测字段 expires_at 和 epoch 的下界；租约过期由服务端的 PEXPIRE
// 决定，不依赖客户端时钟，客户端之间的时钟偏差不会影响正确性。
package leader

import (
	"context"
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
	defaultKeyPrefix = "leader:"
	defaultTTL       = 10 * time.Second
	redisCallTimeout = 2 * time.Second
)

// Event.Reason 的取值。
const (
	ReasonAcquired     = "acquired"
	ReasonTakenOver    = "taken_over"
	ReasonLeaseExpired = "lease_expired"
	ReasonResigned     = "resigned"
	ReasonClosed       = "closed"
)

var (
	// ErrClosed 表示选举已经通过 Resign 或 Close 结束。
	ErrClosed = errors.New("leader election is closed")
)

// acquireScript 在未过期时抢占租约，或在仍是持有者时续租并刷新 TTL。
// 其它持有者存在时不做任何写入，返回当前持有者供调用方观测。
// 任期取 max(INCR, now_ms)：Redis 主从切换丢掉 epoch key 后 INCR 会从 1 重来，
// 用毫秒时间做下界可以避免 fencing token 回退。
var acquireScript = redis.NewScript(`
local owner = redis.call("HGET", KEYS[1], "owner")
if owner and owner ~= ARGV[1] then
	return {0, redis.call("HGET", KEYS[1], "epoch") or "0", owner}
end
local now = tonumber(ARGV[3])
local function next_epoch()
	local counter = redis.call("INCR", KEYS[2])
	if counter < now then
		counter = now
		redis.call("SET", KEYS[2], string.format("%.0f", counter))
	end
	return string.format("%.0f", counter)
end
local epoch
if owner == ARGV[1] then
	epoch = redis.call("HGET", KEYS[1], "epoch")
	if not epoch then
		epoch = next_epoch()
	end
else
	epoch = next_epoch()
	redis.call("HSET", KEYS[1], "acquired_at", ARGV[3])
end
redis.call("HSET", KEYS[1],
	"owner", ARGV[1],
	"epoch", epoch,
	"expires_at", tostring(tonumber(ARGV[3]) + tonumber(ARGV[2])))
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return {1, epoch, ARGV[1]}
`)

// releaseScript 只允许持有者删除租约，避免让位时误删已接管者的租约。
var releaseScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "owner") == ARGV[1] then
	redis.call("DEL", KEYS[1])
	return 1
end
return 0
`)

// Config 描述一次选举。Group 和 Owner 必填。
type Config struct {
	// Addr 是 Redis 单地址，形如 127.0.0.1:6379，不要带 redis:// 前缀。
	// 连接池由库自己创建并在 Close 时关闭，暂不支持 Sentinel/Cluster 拓扑。
	Addr string
	// Password 为空表示无密码。
	Password string
	// DB 是逻辑库编号，选主的 key 落在这个库里；需要和业务读写使用同一个库。
	DB int
	// KeyPrefix 为空时使用 leader:，最终 key 为 <KeyPrefix>{<group>}:owner。
	KeyPrefix string
	// Group 相同的所有进程竞争同一个主节点。
	Group string
	// Owner 必须唯一标识一个进程。
	Owner string
	// TTL 是租约时长，默认 10 秒。
	TTL time.Duration
	// RenewInterval 是当选后的续租间隔，默认 TTL/3，必须小于 TTL。
	RenewInterval time.Duration
	// RetryInterval 是未当选时的重试间隔，默认 TTL/3。
	RetryInterval time.Duration
	// OnChange 在主节点身份变化时调用。
	OnChange func(Event)
}

// Event 描述主节点身份变化。IsLeader 为 true 表示当选，false 表示降级或让位；
// Holder 是变化后 Redis 上的持有者，降级时通常是接管者。
type Event struct {
	IsLeader bool
	Epoch    int64
	Reason   string
	Holder   string
}

// Election 维护一个进程在某个 group 中的主节点身份。
type Election struct {
	client        *redis.Client
	keyPrefix     string
	group         string
	owner         string
	ttl           time.Duration
	renewInterval time.Duration
	retryInterval time.Duration
	onChange      func(Event)

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closed    atomic.Bool

	leader atomic.Bool
	epoch  atomic.Int64

	// opMu 串行化续租与主动让位，避免让位删除租约后，正在执行的续租又把它写回去。
	opMu sync.Mutex
	// leaseTillNano 是本地租约到期的 UnixNano，0 表示没有租约。
	// IsLeader 会在热路径上校验它，续租调用卡住时到期也不会继续返回 true。
	leaseTillNano atomic.Int64
}

// Open 创建选举并启动后台抢占/续租协程。
func Open(ctx context.Context, cfg Config) (*Election, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	cfg.Group = strings.TrimSpace(cfg.Group)
	cfg.Owner = strings.TrimSpace(cfg.Owner)
	if cfg.Addr == "" {
		return nil, errors.New("redis address is empty")
	}
	if cfg.Group == "" {
		return nil, errors.New("leader group is empty")
	}
	// group 直接拼进 hash tag，带花括号会让 Redis Cluster 解析出错误的 slot。
	if strings.ContainsAny(cfg.Group, "{} \t\r\n") {
		return nil, errors.New("leader group must not contain braces or whitespace")
	}
	if cfg.Owner == "" {
		return nil, errors.New("leader owner is empty")
	}
	if cfg.TTL <= 0 {
		cfg.TTL = defaultTTL
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = cfg.TTL / 3
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = cfg.TTL / 3
	}
	if cfg.RenewInterval >= cfg.TTL {
		return nil, errors.New("leader renew interval must be shorter than ttl")
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
		log.Error("主节点选举连接 Redis 失败: addr=%s, db=%d, err=%v", cfg.Addr, cfg.DB, err)
		return nil, fmt.Errorf("connect leader redis: %w", err)
	}

	// 选举生命周期不跟随 Open 的 ctx：启动期 ctx 通常很快取消，跟着它会让
	// 选举在服务运行中途静默停止。
	electionCtx, cancel := context.WithCancel(context.Background())
	election := &Election{
		client:        client,
		keyPrefix:     cfg.KeyPrefix,
		group:         cfg.Group,
		owner:         cfg.Owner,
		ttl:           cfg.TTL,
		renewInterval: cfg.RenewInterval,
		retryInterval: cfg.RetryInterval,
		onChange:      cfg.OnChange,
		ctx:           electionCtx,
		cancel:        cancel,
	}
	election.wg.Add(1)
	go election.run()
	log.Info(
		"主节点选举启动: addr=%s, db=%d, prefix=%s, group=%s, owner=%s, ttl=%s, renew=%s, retry=%s",
		cfg.Addr,
		cfg.DB,
		cfg.KeyPrefix,
		cfg.Group,
		cfg.Owner,
		cfg.TTL,
		cfg.RenewInterval,
		cfg.RetryInterval,
	)
	return election, nil
}

// IsLeader 返回当前是否持有主节点身份，只读本地状态，可在热路径调用。
// 本地租约到期后即使续租调用还没返回也返回 false，不会因为 Redis 卡住而多当
// 一段时间的主节点。
func (e *Election) IsLeader() bool {
	return e != nil && e.leader.Load() && !e.leaseExpired()
}

// Epoch 返回当前任期的 fencing token（毫秒量级），非主节点返回 0。
// 下游写入必须携带并校验。
func (e *Election) Epoch() int64 {
	if e == nil || !e.IsLeader() {
		return 0
	}
	return e.epoch.Load()
}

// Leader 返回当前主节点的 Owner 和任期；没有主节点时返回空字符串和 0。
func (e *Election) Leader(ctx context.Context) (string, int64, error) {
	if e == nil {
		return "", 0, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := withRedisTimeout(ctx)
	defer cancel()

	values, err := e.client.HMGet(callCtx, e.ownerKey(), "owner", "epoch").Result()
	if err != nil {
		return "", 0, err
	}
	if len(values) != 2 {
		return "", 0, nil
	}
	epoch, err := redisInt64(values[1])
	if err != nil {
		return "", 0, err
	}
	return redisString(values[0]), epoch, nil
}

// Resign 主动释放租约并永久停止本次选举：之后 IsLeader 恒为 false，不再抢占。
// 需要重新参与选举必须再次调用 Open。释放失败只记录日志，本地状态照样降级。
func (e *Election) Resign(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// 与 step 共用 opMu：让位期间不会插进一次续租把租约写回去，
	// 也不用等待后台协程退出，因此在 OnChange 回调里调用也不会自锁。
	e.opMu.Lock()
	if e.closed.Load() {
		e.opMu.Unlock()
		return ErrClosed
	}
	e.cancel()
	releaseCtx, releaseCancel := withRedisTimeout(context.WithoutCancel(ctx))
	released, err := e.release(releaseCtx)
	releaseCancel()
	event := e.demote(ReasonResigned, 0, "")
	e.opMu.Unlock()

	if err != nil {
		log.Error("主动让位释放主节点租约失败: group=%s, owner=%s, err=%v", e.group, e.owner, err)
	} else if !released {
		log.Warn("主动让位时已不是租约持有者: group=%s, owner=%s", e.group, e.owner)
	}
	if event != nil {
		e.emit(*event)
	}
	return err
}

// Close 停止选举并在仍持有租约时尽力释放，让其它节点立即接管。
func (e *Election) Close() error {
	if e == nil {
		return nil
	}
	var closeErr error
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		e.cancel()
		e.wg.Wait()

		e.opMu.Lock()
		event := e.demote(ReasonClosed, 0, "")
		e.opMu.Unlock()

		if event != nil {
			releaseCtx, cancel := withRedisTimeout(context.Background())
			released, err := e.release(releaseCtx)
			cancel()
			if err != nil {
				log.Error("关闭时释放主节点租约失败: group=%s, owner=%s, err=%v", e.group, e.owner, err)
				closeErr = err
			} else if !released {
				log.Warn("关闭时已不是租约持有者: group=%s, owner=%s", e.group, e.owner)
			}
			e.emit(*event)
		}
		log.Info("主节点选举关闭: group=%s, owner=%s", e.group, e.owner)
		if err := e.client.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

func (e *Election) run() {
	defer e.wg.Done()

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-timer.C:
		}

		wait, event := e.step()
		if event != nil {
			e.emit(*event)
		}
		if e.ctx.Err() != nil {
			return
		}
		timer.Reset(wait)
	}
}

// step 执行一次抢占或续租，返回下一轮的等待时间和需要通知的身份变化。
func (e *Election) step() (time.Duration, *Event) {
	e.opMu.Lock()
	defer e.opMu.Unlock()

	if e.closed.Load() || e.ctx.Err() != nil {
		return e.ttl, nil
	}
	start := time.Now()
	ctx, cancel := withRedisTimeout(e.ctx)
	acquired, epoch, holder, err := e.acquire(ctx)
	cancel()

	if err != nil {
		if e.ctx.Err() != nil {
			return e.ttl, nil
		}
		if e.leader.Load() {
			log.Warn("主节点续租失败: group=%s, owner=%s, err=%v", e.group, e.owner, err)
			if e.leaseExpired() {
				return e.retryInterval, e.demote(ReasonLeaseExpired, 0, "")
			}
			return e.renewInterval, nil
		}
		log.Warn("主节点抢占失败: group=%s, owner=%s, err=%v", e.group, e.owner, err)
		return e.retryInterval, nil
	}

	if acquired {
		wasLeader := e.leader.Swap(true)
		e.epoch.Store(epoch)
		// 用请求发起时间计算本地租约，保证本地到期不晚于 Redis 侧到期，
		// Redis 不可用时不会比真实租约多当一小段时间的主节点。
		e.setLeaseTill(start.Add(e.ttl))
		if e.leaseExpired() {
			// 续租往返时间已经超过租期，Redis 侧租约同样已经过期，不能继续当主节点。
			return e.retryInterval, e.demote(ReasonLeaseExpired, 0, "")
		}
		if !wasLeader {
			log.Info("当选主节点: group=%s, owner=%s, epoch=%d", e.group, e.owner, epoch)
			return e.renewInterval, &Event{IsLeader: true, Epoch: epoch, Reason: ReasonAcquired, Holder: e.owner}
		}
		return e.renewInterval, nil
	}

	if e.leader.Load() {
		return e.retryInterval, e.demote(ReasonTakenOver, epoch, holder)
	}
	return e.retryInterval, nil
}

func (e *Election) acquire(ctx context.Context) (bool, int64, string, error) {
	values, err := acquireScript.Run(
		ctx,
		e.client,
		[]string{e.ownerKey(), e.epochKey()},
		e.owner,
		e.ttl.Milliseconds(),
		time.Now().UnixMilli(),
	).Slice()
	if err != nil {
		return false, 0, "", err
	}
	if len(values) != 3 {
		return false, 0, "", fmt.Errorf("unexpected leader script reply length %d", len(values))
	}
	status, err := redisInt64(values[0])
	if err != nil {
		return false, 0, "", err
	}
	epoch, err := redisInt64(values[1])
	if err != nil {
		return false, 0, "", err
	}
	return status == 1, epoch, redisString(values[2]), nil
}

func (e *Election) release(ctx context.Context) (bool, error) {
	n, err := releaseScript.Run(ctx, e.client, []string{e.ownerKey()}, e.owner).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// demote 清零本地主节点状态并返回需要通知的事件；本来就不是主节点时返回 nil。
func (e *Election) demote(reason string, epoch int64, holder string) *Event {
	if !e.leader.Swap(false) {
		return nil
	}
	previous := e.epoch.Swap(0)
	e.setLeaseTill(time.Time{})
	if epoch == 0 {
		epoch = previous
	}
	log.Warn(
		"失去主节点身份: group=%s, owner=%s, epoch=%d, reason=%s, holder=%s",
		e.group,
		e.owner,
		epoch,
		reason,
		holder,
	)
	return &Event{IsLeader: false, Epoch: epoch, Reason: reason, Holder: holder}
}

func (e *Election) emit(event Event) {
	if e.onChange != nil {
		e.onChange(event)
	}
}

func (e *Election) setLeaseTill(t time.Time) {
	if t.IsZero() {
		e.leaseTillNano.Store(0)
		return
	}
	e.leaseTillNano.Store(t.UnixNano())
}

func (e *Election) leaseExpired() bool {
	deadline := e.leaseTillNano.Load()
	return deadline == 0 || time.Now().UnixNano() >= deadline
}

func (e *Election) ownerKey() string {
	return e.keyPrefix + "{" + e.group + "}:owner"
}

func (e *Election) epochKey() string {
	return e.keyPrefix + "{" + e.group + "}:epoch"
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

func redisString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func redisInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case nil:
		return 0, nil
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
