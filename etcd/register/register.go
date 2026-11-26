// Package register 提供基于 etcd 租约的服务注册、保活和重新注册。
//
// # 静态注册
//
//	reg, err := register.NewRegister(
//		"127.0.0.1:2379,127.0.0.2:2379",
//		register.RegisterOption{
//			Key:   "services/gateway/gw-01",
//			Value: "10.0.0.8:8080",
//			TTL:   10,
//		},
//	)
//	if err != nil {
//		return err
//	}
//	defer reg.Close()
//
//	if err := reg.Register(); err != nil {
//		return err
//	}
//
// NewRegister 只创建 etcd 客户端；Register 才会同步创建租约、写入 key/value
// 并启动 keepalive。endpoints 使用逗号分隔。TTL、DialTimeout 和 GrantTimeout
// 未设置时分别使用 10 秒、5 秒和 5 秒。
// 首次注册和重新注册都会读取目标 key，再用 revision 条件事务写入。
// 启用 etcd RBAC 时，注册账号需要目标 key 的读写权限（readwrite）。
//
// # 动态注册
//
// 地址或实例 ID 可能变化时使用 KeyFunc/ValueFunc：
//
//	reg, err := register.NewRegister(endpoint, register.RegisterOption{
//		KeyFunc: func() string {
//			return "services/gateway/" + currentInstanceID()
//		},
//		ValueFunc: func() string {
//			return currentAdvertiseAddr()
//		},
//		TTL: 10,
//		OnLeaseLost: func(err error) {
//			log.Error("registration lost: %v", err)
//		},
//	})
//
// 函数值优先于静态 Key/Value，并在首次注册和重新注册时重新求值。keepalive
// 中断后注册器会退避重试，直到恢复或关闭；首次自动恢复失败时调用 OnLeaseLost。
//
// IsAlive 表示注册器当前在本地确认注册有效。替换结果未确认时返回 false，
// 即使仍在给旧租约保活以便回滚。它不能代替端到端健康检查。
// LeaseID 可用于诊断。ReRegister 写入新注册后才停止并撤销旧租约。Close 应在进程
// 退出时调用，它会取消保活、尽力撤销租约并关闭 etcd 客户端。
package register

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyao-lib/go/log"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// RegisterOption - 服务注册选项
// ---------------------------------------------------------------------------

// RegisterOption 通用服务注册配置选项。
//
// Key 与 KeyFunc、Value 与 ValueFunc 二选一即可：
//   - 静态值：直接设置 Key / Value
//   - 动态值：设置 KeyFunc / ValueFunc，每次注册/重注册时动态生成
//
// 同一注册器串行执行动态回调，Close 无需等待这些回调返回。
//
// DialTimeout 和 GrantTimeout 默认均为 5 秒。
// AutoReRegister 默认为 true，租约丢失后自动重新注册。
// OnLeaseLost 在首次自动重注册失败时触发（AutoReRegister=true），
// 或 keepalive 断开时直接触发（AutoReRegister=false）。
type RegisterOption struct {
	Key   string // etcd key，与 KeyFunc 互斥
	Value string // etcd value，与 ValueFunc 互斥

	KeyFunc   func() string // 动态生成 key（优先级高于 Key）
	ValueFunc func() string // 动态生成 value（优先级高于 Value）

	TTL int // 租约 TTL（秒），默认 10

	// DialTimeout etcd 客户端连接超时，默认 5 秒。
	DialTimeout time.Duration

	// GrantTimeout 读取注册记录、创建租约和写入事务的共同超时，默认 5 秒。
	GrantTimeout time.Duration

	// AutoReRegister 租约丢失后是否自动重新注册。
	// 由 setDefaults 根据 DisableAutoReRegister 计算得出（默认 true），
	// 调用方不要直接赋值，否则会在 setDefaults 中被覆盖。
	AutoReRegister bool

	// DisableAutoReRegister 显式关闭自动重新注册。
	// 旧实现无条件把 AutoReRegister 置为 true，调用方设 false 会被静默覆盖；
	// 需要关闭时请设置本字段。
	DisableAutoReRegister bool

	// OnLeaseLost 租约丢失回调。
	//   - AutoReRegister=true ：首次自动重注册失败时异步通知，重试继续进行
	//   - AutoReRegister=false：keepalive 断开时直接触发，err 为 nil
	OnLeaseLost func(err error)
}

func (o *RegisterOption) getKey() string {
	if o.KeyFunc != nil {
		return o.KeyFunc()
	}
	return o.Key
}

func (o *RegisterOption) getValue() string {
	if o.ValueFunc != nil {
		return o.ValueFunc()
	}
	return o.Value
}

func (o *RegisterOption) validate() error {
	if o.getKey() == "" {
		return errors.New("register key is empty")
	}
	return nil
}

func (o *RegisterOption) setDefaults() {
	if o.TTL <= 0 {
		o.TTL = 10
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.GrantTimeout <= 0 {
		o.GrantTimeout = 5 * time.Second
	}
	// AutoReRegister 由 DisableAutoReRegister 取反得出，避免旧实现
	// 无条件覆盖调用方传值（传 false 会被静默改成 true）。
	o.AutoReRegister = !o.DisableAutoReRegister
}

// revokeTimeout 是撤销租约的超时时间。
// Revoke(context.Background()) 在 etcd 不可达时会永久阻塞，导致 Close 卡死、
// 自动重注册协程永久挂起，因此所有撤销操作都必须带超时。
const revokeTimeout = 3 * time.Second

const (
	initialRetryDelay = 250 * time.Millisecond
	maxRetryDelay     = 5 * time.Second
)

// ErrRegistrationReplaced 表示同一个 key 已被另一个租约接管。
var ErrRegistrationReplaced = errors.New("etcd registration has been replaced")

// ErrRegistrationCleanup 表示新记录已注册，但仍需在后台恢复旧记录或撤销候选租约。
var ErrRegistrationCleanup = errors.New("etcd registration succeeded with pending cleanup")

// ---------------------------------------------------------------------------
// Register - 通用 etcd 注册器
// ---------------------------------------------------------------------------

// Register 通用 etcd 服务注册器。
type Register struct {
	client              *clientv3.Client
	opt                 RegisterOption
	leaseID             clientv3.LeaseID
	registeredKey       string
	lastAttemptKey      string
	ownership           map[string]*registrationOwnership // protected by mu, including previously used keys
	ownershipVersion    uint64                            // protects checks spanning an unlocked Get
	keepAliveCh         <-chan *clientv3.LeaseKeepAliveResponse
	keepAliveCancel     context.CancelFunc
	recovering          *registrationRetry
	cleanupRetryRunning bool
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	registrationSlot    chan struct{} // serializes callbacks and registration attempts independently of Close
	health              atomic.Pointer[registrationHealth]
	closeOnce           sync.Once
	closeErr            error
}

type registrationOwnership struct {
	leases            map[clientv3.LeaseID]registrationLeaseState
	initial           *registrationSnapshot // incumbent observed before the first write to this key
	pending           map[clientv3.LeaseID]*uncertainRegistration
	emptyKeyCandidate clientv3.LeaseID // a failed write guarded by revision zero, which can match again
	replaced          bool
}

func (o *registrationOwnership) hasUnconfirmedCandidate() bool {
	return len(o.pending) != 0 || o.emptyKeyCandidate != 0
}

// Snapshot the rollback chain while holding Register.mu.
func (o *registrationOwnership) rollbackTargets(attempt *uncertainRegistration) []registrationSnapshot {
	targets := []registrationSnapshot{attempt.previous}
	for predecessor := o.pending[attempt.previous.lease]; predecessor != nil; predecessor = o.pending[predecessor.previous.lease] {
		targets = append(targets, predecessor.previous)
	}
	return targets
}

type registrationHealth struct {
	keepAliveCtx         context.Context
	registered           atomic.Bool
	checkingOwnership    bool               // protected by Register.mu
	cancelOwnershipCheck context.CancelFunc // protected by Register.mu
}

type registrationLeaseState uint8

const (
	confirmedLease         registrationLeaseState = iota
	uncertainLease                                // may require rollback before revocation
	candidateRevokePending                        // can retry revocation without a rollback
)

type registrationSnapshot struct {
	lease    clientv3.LeaseID
	revision int64
	value    string
}

type uncertainRegistration struct {
	lease           clientv3.LeaseID
	previous        registrationSnapshot
	restoreOnRetire bool // the predecessor belongs to another registrar
	retryRestore    bool // a newer attempt settled and may have restored this candidate
}

func (a *uncertainRegistration) fencedBy(current registrationSnapshot) bool {
	return current.revision != a.previous.revision && current.lease != a.lease
}

type registrationRetry struct {
	firstErr error // non-nil when ReRegister already attempted to replace the lease
}

// NewRegister 创建 etcd 注册器并连接 etcd。
//
//	reg, err := etcd.NewRegister("127.0.0.1:2379", etcd.RegisterOption{
//	    Key:   "my-service/my-instance",
//	    Value: "1.2.3.4:8080",
//	    TTL:   10,
//	})
func NewRegister(endpoints string, opt RegisterOption) (*Register, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	opt.setDefaults()

	endpointList := strings.Split(endpoints, ",")
	for i := range endpointList {
		endpointList[i] = strings.TrimSpace(endpointList[i])
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpointList,
		DialTimeout: opt.DialTimeout,
	})
	if err != nil {
		log.Error("创建 etcd 客户端失败: endpoints=%s, err=%v", endpoints, err)
		return nil, fmt.Errorf("etcd client error: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	reg := &Register{
		client:           cli,
		opt:              opt,
		ownership:        make(map[string]*registrationOwnership),
		ctx:              ctx,
		cancel:           cancel,
		registrationSlot: make(chan struct{}, 1),
	}
	log.Info(
		"注册器创建成功: endpoints=%s, key=%s, ttl=%ds",
		endpoints,
		opt.getKey(),
		opt.TTL,
	)
	return reg, nil
}

// Register 执行首次注册，创建租约、启动保活并写入注册记录。
// 注册账号需要读取和写入注册 key 的权限。
// 已完成注册时直接返回。需要刷新地址或注册内容时使用 ReRegister。
// ErrRegistrationCleanup 表示注册已生效，旧记录清理会在后台重试。
func (r *Register) Register() error {
	return r.runRegistration(false, nil)
}

func (r *Register) runRegistration(replace bool, retry *registrationRetry) error {
	select {
	case r.registrationSlot <- struct{}{}:
		defer func() { <-r.registrationSlot }()
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	skip := (!replace && r.IsAlive()) || (retry != nil && r.recovering != retry)
	r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if skip {
		return nil
	}
	key := r.opt.getKey()
	if err := r.ctx.Err(); err != nil {
		return err
	}
	value := r.opt.getValue()

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if retry != nil && r.recovering != retry {
		return nil
	}
	err := r.doRegister(key, value)
	if err == nil || errors.Is(err, ErrRegistrationCleanup) {
		return err
	}
	if retry != nil {
		if errors.Is(err, ErrRegistrationReplaced) {
			r.recovering = nil
		}
	} else if replace {
		log.Error("服务重新注册失败: key=%s, err=%v", key, err)
		if errors.Is(err, ErrRegistrationReplaced) {
			r.recovering = nil
		} else if r.opt.AutoReRegister && r.ctx.Err() == nil {
			retry := &registrationRetry{firstErr: err}
			r.recovering = retry
			go r.retryRegistration(retry)
		}
	} else {
		log.Error("服务注册失败: key=%s, err=%v", key, err)
	}
	return err
}

// doRegister 内部注册逻辑，调用方持有 r.mu。
func (r *Register) doRegister(key, value string) error {
	r.ownershipVersion++
	if health := r.health.Load(); health != nil && health.cancelOwnershipCheck != nil {
		health.cancelOwnershipCheck()
	}
	r.lastAttemptKey = key

	writeCtx, cancel := context.WithTimeout(r.ctx, r.opt.GrantTimeout)
	defer cancel()
	ownership := r.ownership[key]
	previous, err := r.readRegistration(writeCtx, key, ownership)
	if err != nil {
		return err
	}
	if ownership == nil {
		ownership = &registrationOwnership{
			leases:  make(map[clientv3.LeaseID]registrationLeaseState),
			initial: &previous,
			pending: make(map[clientv3.LeaseID]*uncertainRegistration),
		}
		r.ownership[key] = ownership
	}
	leaseResp, err := r.client.Grant(writeCtx, int64(r.opt.TTL))
	if err != nil {
		return fmt.Errorf("etcd grant lease error: %w", err)
	}
	leaseID := leaseResp.ID
	keepAliveCtx, stopKeepAlive := context.WithCancel(r.ctx)
	keepAliveCh, err := r.client.KeepAlive(keepAliveCtx, leaseID)
	if err != nil {
		stopKeepAlive()
		result := fmt.Errorf("etcd keepalive error: %w", err)
		if revokeErr := r.revokeLease(r.ctx, leaseID); revokeErr != nil {
			r.queueCandidateRevoke(ownership, leaseID)
			result = errors.Join(result, fmt.Errorf("revoke etcd candidate lease %d: %w", leaseID, revokeErr))
		}
		return result
	}
	if writeErr := r.putRegistration(writeCtx, key, value, leaseID, previous.revision); writeErr != nil {
		// The client can retry after a lost reply and return a different
		// status, even though the first transaction committed.
		var confirmed bool
		var checkErr error
		if previous.revision == 0 && r.ctx.Err() == nil {
			confirmed, checkErr = r.confirmCandidate(key, value, leaseID)
		}
		if !confirmed {
			stopKeepAlive()
			activeKey := r.registeredKey == key
			if activeKey {
				r.setRegistered(false)
			}
			_, previousOwned := ownership.leases[previous.lease]
			attempt := uncertainRegistration{
				lease: leaseID, previous: previous, restoreOnRetire: previous.revision != 0 && !previousOwned,
			}
			unresolved, cleanupErr := true, checkErr
			if checkErr == nil {
				unresolved, cleanupErr = r.resolveUnconfirmed(r.ctx, key, &attempt, ownership.rollbackTargets(&attempt), r.healthForLease(key, r.leaseID))
			} else {
				log.Warn("确认候选租约失败: key=%s, lease=%d, err=%v", key, leaseID, checkErr)
			}
			if previous.revision == 0 && cleanupErr != nil {
				ownership.emptyKeyCandidate = leaseID
				r.queueCandidateRevoke(ownership, leaseID)
			} else if unresolved {
				ownership.leases[leaseID] = uncertainLease
				ownership.pending[leaseID] = &attempt
			} else if cleanupErr != nil {
				r.queueCandidateRevoke(ownership, leaseID)
			}
			if predecessor := ownership.pending[previous.lease]; predecessor != nil && !unresolved {
				predecessor.retryRestore = true
			}
			err := fmt.Errorf("etcd put error: %w", writeErr)
			if cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("clean up etcd candidate lease %d: %w", leaseID, cleanupErr))
			}
			if activeKey {
				if checkErr := r.recheckActiveRegistration(key, ownership); checkErr != nil {
					err = errors.Join(err, checkErr)
				}
			}
			return err
		}
	}

	previousCancel := r.keepAliveCancel
	r.leaseID = leaseID
	r.registeredKey = key
	ownership.initial = nil
	clear(ownership.pending)
	ownership.leases[leaseID] = confirmedLease
	r.keepAliveCh = keepAliveCh
	r.keepAliveCancel = stopKeepAlive
	r.recovering = nil
	health := &registrationHealth{keepAliveCtx: keepAliveCtx}
	health.registered.Store(true)
	r.health.Store(health)
	log.Info("服务注册成功: %s = %s (TTL=%ds)", key, value, r.opt.TTL)
	go r.watchKeepAlive(keepAliveCtx, stopKeepAlive, keepAliveCh)
	if previousCancel != nil {
		previousCancel()
	}
	if cleanupErr, pending := r.revokeRetiredLeases(r.ctx, leaseID, false); pending {
		r.startCleanupRetryLocked()
		log.Warn("服务已注册，旧记录清理待重试: key=%s, err=%v", key, cleanupErr)
		return fmt.Errorf("%w: %w", ErrRegistrationCleanup, cleanupErr)
	}
	return nil
}

func (r *Register) getRegistration(ctx context.Context, key string) (registrationSnapshot, error) {
	current, err := r.client.Get(ctx, key)
	if err != nil {
		return registrationSnapshot{}, fmt.Errorf("etcd read current registration: %w", err)
	}
	if len(current.Kvs) == 0 {
		return registrationSnapshot{}, nil
	}
	entry := current.Kvs[0]
	return registrationSnapshot{lease: clientv3.LeaseID(entry.Lease), revision: entry.ModRevision, value: string(entry.Value)}, nil
}

// Recheck after rollback because a failed conditional write can mean another
// registrar took over while the old keepalive stream was still open.
func (r *Register) recheckActiveRegistration(key string, ownership *registrationOwnership) error {
	ctx, cancel := context.WithTimeout(r.ctx, revokeTimeout)
	defer cancel()
	current, err := r.getRegistration(ctx, key)
	if err != nil {
		r.startOwnershipCheckLocked(key)
		return err
	}
	if err := r.applyRegistrationCheck(key, ownership, current); err != nil {
		return err
	}
	if ownership.hasUnconfirmedCandidate() {
		r.startOwnershipCheckLocked(key)
	}
	return nil
}

func (r *Register) applyRegistrationCheck(key string, ownership *registrationOwnership, current registrationSnapshot) error {
	if err := r.checkOwnership(key, ownership, current); err != nil {
		return err
	}
	r.retireFencedCandidates(ownership, current)
	r.setRegistered(current.revision != 0 && current.lease == r.leaseID && len(ownership.pending) == 0)
	return nil
}

// A candidate can still be restored by a newer pending rollback even when it
// no longer owns the key. Keep the predecessors of every unfenced attempt.
func (r *Register) retireFencedCandidates(ownership *registrationOwnership, current registrationSnapshot) {
	if len(ownership.pending) == 0 {
		return
	}
	reachable := make(map[clientv3.LeaseID]struct{}, len(ownership.pending))
	for _, attempt := range ownership.pending {
		if attempt.fencedBy(current) {
			continue
		}
		for candidate := attempt; candidate != nil; candidate = ownership.pending[candidate.previous.lease] {
			if _, seen := reachable[candidate.lease]; seen {
				break
			}
			reachable[candidate.lease] = struct{}{}
		}
	}
	for lease, attempt := range ownership.pending {
		if _, keep := reachable[lease]; keep {
			continue
		}
		if predecessor := ownership.pending[attempt.previous.lease]; predecessor != nil && predecessor.lease == current.lease {
			predecessor.retryRestore = true
		}
		delete(ownership.pending, lease)
		r.queueCandidateRevoke(ownership, lease)
	}
}

func (r *Register) startOwnershipCheckLocked(key string) {
	health := r.health.Load()
	if r.registeredKey == key && health != nil && health.keepAliveCtx.Err() == nil && !health.checkingOwnership {
		health.checkingOwnership = true
		go r.retryOwnershipCheck(key, health)
	}
}

func (r *Register) retryOwnershipCheck(key string, health *registrationHealth) {
	for delay := initialRetryDelay; ; delay = min(delay*2, maxRetryDelay) {
		timer := time.NewTimer(delay)
		select {
		case <-health.keepAliveCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		r.mu.Lock()
		if r.health.Load() != health || health.keepAliveCtx.Err() != nil {
			r.mu.Unlock()
			return
		}
		version := r.ownershipVersion
		ctx, cancel := context.WithTimeout(health.keepAliveCtx, revokeTimeout)
		health.cancelOwnershipCheck = cancel
		r.mu.Unlock()

		current, err := r.getRegistration(ctx, key)
		cancel()

		r.mu.Lock()
		health.cancelOwnershipCheck = nil
		if r.health.Load() != health || health.keepAliveCtx.Err() != nil {
			r.mu.Unlock()
			return
		}
		var rollbackTargets []registrationSnapshot
		if err == nil && r.ownershipVersion == version {
			owner := r.ownership[key]
			if err := r.applyRegistrationCheck(key, owner, current); err != nil || !owner.hasUnconfirmedCandidate() {
				health.checkingOwnership = false
				r.mu.Unlock()
				return
			}
			if attempt := owner.pending[current.lease]; attempt != nil && attempt.retryRestore {
				rollbackTargets = owner.rollbackTargets(attempt)
				ctx, cancel = context.WithTimeout(health.keepAliveCtx, revokeTimeout)
				health.cancelOwnershipCheck = cancel
			}
		}
		r.mu.Unlock()
		if len(rollbackTargets) != 0 {
			// Its original rollback may have raced the newer candidate and
			// missed. Restore only from the revision we just observed.
			restored, lease, err := r.restoreRegistration(ctx, key, clientv3.Compare(clientv3.ModRevision(key), "=", current.revision), rollbackTargets)
			cancel()
			r.mu.Lock()
			health.cancelOwnershipCheck = nil
			if err == nil && restored && r.health.Load() == health && r.ownershipVersion == version && r.leaseID == lease {
				health.registered.Store(true)
			}
			r.mu.Unlock()
		}
	}
}

func (r *Register) checkOwnership(key string, ownership *registrationOwnership, current registrationSnapshot) error {
	if current.revision == 0 {
		return nil
	}
	_, own := ownership.leases[current.lease]
	initial := ownership.initial
	// The first registration may replace the incumbent it observed, but a
	// retry must leave a newer registrar's record alone.
	if own || (initial != nil && initial.revision != 0 && initial.lease == current.lease) {
		return nil
	}
	ownership.replaced = true
	r.retireFencedCandidates(ownership, current)
	if r.registeredKey == key {
		r.stopKeepAliveLocked()
	}
	return fmt.Errorf("%w: %s", ErrRegistrationReplaced, key)
}

// readRegistration resolves an earlier uncertain write before starting a new
// attempt for the key. It returns the exact revision the next write must match.
func (r *Register) readRegistration(ctx context.Context, key string, ownership *registrationOwnership) (registrationSnapshot, error) {
	if ownership != nil && ownership.replaced {
		return registrationSnapshot{}, fmt.Errorf("%w: %s", ErrRegistrationReplaced, key)
	}
	for {
		previous, err := r.getRegistration(ctx, key)
		if err != nil {
			r.startOwnershipCheckLocked(key)
			return registrationSnapshot{}, err
		}
		if ownership == nil {
			return previous, nil
		}
		if err := r.checkOwnership(key, ownership, previous); err != nil {
			return registrationSnapshot{}, err
		}
		if candidate := ownership.emptyKeyCandidate; candidate != 0 {
			// An absent key can become absent again. Only revocation prevents
			// a late revision-zero write from attaching to this candidate.
			if err := r.revokeLease(ctx, candidate); err != nil {
				return registrationSnapshot{}, fmt.Errorf("revoke unresolved etcd lease %d before retrying key %q: %w", candidate, key, err)
			}
			delete(ownership.leases, candidate)
			ownership.emptyKeyCandidate = 0
			continue // the candidate may have owned the key before revocation
		}
		r.retireFencedCandidates(ownership, previous)
		pending := ownership.pending[previous.lease]
		if pending != nil && !pending.restoreOnRetire {
			// A successful replacement can retire an owned predecessor and
			// its candidate together. Keep both snapshots if it fails.
			return previous, nil
		}
		if pending == nil {
			for _, attempt := range ownership.pending {
				pending = attempt
				break
			}
		}
		if pending != nil {
			unresolved, err := r.resolveUnconfirmed(ctx, key, pending, ownership.rollbackTargets(pending), r.healthForLease(key, r.leaseID))
			if unresolved {
				return registrationSnapshot{}, fmt.Errorf("etcd resolve unconfirmed registration: %w", err)
			}
			if err == nil {
				delete(ownership.leases, pending.lease)
			} else {
				r.queueCandidateRevoke(ownership, pending.lease)
			}
			delete(ownership.pending, pending.lease)
			continue // a successful rollback can change the key revision
		}
		if r.registeredKey == key {
			r.setRegistered(previous.revision != 0 && previous.lease == r.leaseID)
		}
		return previous, nil
	}
}

func (r *Register) putRegistration(ctx context.Context, key, value string, candidate clientv3.LeaseID, revision int64) error {
	// Revision zero guards a missing key against a successor registering
	// after Get but before this attempt writes its replacement.
	result, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(candidate))).
		Commit()
	if err == nil && !result.Succeeded {
		// A retry after a lost reply also sees a comparison failure if the
		// first transaction applied. Reconcile before revoking the candidate.
		return status.Error(codes.Aborted, "current registration changed during replacement")
	}
	return err
}

// confirmCandidate handles an attempt that began with an empty key. There is
// no predecessor to restore if the candidate already owns the desired value.
func (r *Register) confirmCandidate(key, value string, candidate clientv3.LeaseID) (bool, error) {
	ctx, cancel := context.WithTimeout(r.ctx, revokeTimeout)
	defer cancel()
	current, err := r.getRegistration(ctx, key)
	if err != nil {
		return false, err
	}
	if current.revision == 0 || current.lease != candidate {
		return false, nil
	}
	if current.value != value {
		return false, errors.New("candidate registration value changed before confirmation")
	}
	return true, nil
}

// restoreRegistration keeps the same compare while skipping expired predecessors.
func (r *Register) restoreRegistration(ctx context.Context, key string, cmp clientv3.Cmp, targets []registrationSnapshot) (restored bool, lease clientv3.LeaseID, err error) {
	for _, target := range targets {
		lease = target.lease
		result, putErr := r.client.Txn(ctx).If(cmp).
			Then(clientv3.OpPut(key, target.value, clientv3.WithLease(lease))).Commit()
		err = putErr
		if err == nil {
			return result.Succeeded, lease, nil
		}
		if lease == 0 || !errors.Is(err, rpctypes.ErrLeaseNotFound) {
			break
		}
	}
	return false, lease, err
}

// resolveUnconfirmed reports whether rollback is still needed before revocation.
// When the predecessor is the active lease, publish its health as soon as etcd
// acknowledges the rollback.
func (r *Register) resolveUnconfirmed(parent context.Context, key string, attempt *uncertainRegistration, targets []registrationSnapshot, restoreHealth *registrationHealth) (unresolved bool, err error) {
	ctx, cancel := context.WithTimeout(parent, revokeTimeout)
	defer cancel()
	if attempt.previous.revision == 0 {
		return false, r.revokeLease(ctx, attempt.lease)
	}
	// Touch the original revision first so an in-flight replacement cannot
	// attach to the candidate after the check. If it already attached, restore
	// only when the key still belongs to the candidate.
	for _, cmp := range []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(key), "=", attempt.previous.revision),
		clientv3.Compare(clientv3.LeaseValue(key), "=", int64(attempt.lease)),
	} {
		restored, lease, err := r.restoreRegistration(ctx, key, cmp, targets)
		if err != nil {
			if lease != 0 && errors.Is(err, rpctypes.ErrLeaseNotFound) {
				// Every fallback lease has expired; fence the late write by
				// revoking the candidate itself.
				return false, r.revokeLease(ctx, attempt.lease)
			}
			if lease != attempt.previous.lease {
				attempt.retryRestore = true
			}
			log.Warn("恢复原注册失败，保留未确认候选租约: key=%s, lease=%d, err=%v", key, attempt.lease, err)
			return true, err
		}
		if restored {
			if restoreHealth != nil && r.leaseID == lease {
				restoreHealth.registered.Store(true)
			}
			break
		}
	}
	// The candidate can no longer be attached by the delayed write. Revocation
	// can fall back to TTL even if the RPC fails.
	return false, r.revokeLease(ctx, attempt.lease)
}

func (r *Register) watchKeepAlive(ctx context.Context, stop context.CancelFunc, keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case _, ok := <-keepAliveCh:
			if !ok {
				// Publish the loss of this lease before waiting for the state lock.
				stop()
				r.handleLeaseLost(keepAliveCh)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Register) handleLeaseLost(keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse) {
	r.mu.Lock()
	if r.ctx.Err() != nil || r.keepAliveCh != keepAliveCh {
		r.mu.Unlock()
		return
	}
	r.stopKeepAliveLocked()
	var retry *registrationRetry
	if r.opt.AutoReRegister {
		retry = &registrationRetry{}
		r.recovering = retry
	}
	log.Error("keepalive 通道关闭，服务注册可能已失效: %s", r.registeredKey)
	r.mu.Unlock()

	if retry == nil {
		r.notifyLeaseLost(nil)
		return
	}
	r.retryRegistration(retry)
}

func (r *Register) retryRegistration(retry *registrationRetry) {
	err := retry.firstErr
	delay := initialRetryDelay
	notified := false
	for {
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			log.Error("自动重注册失败: %v", err)
			if !notified {
				notified = true
				go r.notifyLeaseLost(err)
			}
			if errors.Is(err, ErrRegistrationReplaced) {
				return
			}
			timer := time.NewTimer(delay)
			select {
			case <-r.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay = min(delay*2, maxRetryDelay)
		}
		err = r.runRegistration(true, retry)
		if err == nil || errors.Is(err, ErrRegistrationCleanup) {
			return
		}
	}
}

func (r *Register) notifyLeaseLost(err error) {
	if r.ctx.Err() != nil || r.opt.OnLeaseLost == nil {
		return
	}
	defer func() {
		if value := recover(); value != nil {
			log.Error("OnLeaseLost 回调 panic: %v", value)
		}
	}()
	r.opt.OnLeaseLost(err)
}

// ReRegister 写入新注册成功后停止旧租约保活并尝试撤销。撤销失败时旧租约按 TTL 过期。
// 新注册失败会保留原租约和保活。有自动重注册时，后台会继续退避重试并通知 OnLeaseLost。
// 同一个 key 已被其它租约接管时，返回 ErrRegistrationReplaced 并停止恢复该记录。
// ErrRegistrationCleanup 表示新注册已生效，仅旧记录清理会在后台重试。
func (r *Register) ReRegister() error {
	if r.ctx.Err() != nil {
		return errors.New("register is closed")
	}
	return r.runRegistration(true, nil)
}

func (r *Register) healthForLease(key string, leaseID clientv3.LeaseID) *registrationHealth {
	if r.registeredKey == key && r.leaseID == leaseID {
		return r.health.Load()
	}
	return nil
}

func (r *Register) setRegistered(registered bool) {
	if health := r.health.Load(); health != nil {
		health.registered.Store(registered)
	}
}

func (r *Register) stopKeepAliveLocked() {
	r.health.Store(nil)
	r.keepAliveCh = nil
	if r.keepAliveCancel != nil {
		r.keepAliveCancel()
		r.keepAliveCancel = nil
	}
}

// revokeLease 在限定时间内撤销指定租约，并记录和返回撤销错误。
func (r *Register) revokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	revokeCtx, cancel := context.WithTimeout(ctx, revokeTimeout)
	defer cancel()
	if _, err := r.client.Revoke(revokeCtx, leaseID); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		log.Error("撤销租约失败: lease=%d, err=%v", leaseID, err)
		return err
	}
	return nil
}

// revokeRetiredLeases is called under r.mu. Foreign predecessor restoration and
// lease revocation share one deadline. During a background retry, only leases
// already scheduled for revocation or needing foreign restoration are eligible.
func (r *Register) revokeRetiredLeases(parent context.Context, keep clientv3.LeaseID, retryOnly bool) (err error, cleanupPending bool) {
	type retiredLease struct {
		owner   *registrationOwnership
		id      clientv3.LeaseID
		key     string
		state   registrationLeaseState
		restore *uncertainRegistration
		targets []registrationSnapshot
		err     error
		revoked bool
		settled bool
	}
	var retired []retiredLease
	for key, owner := range r.ownership {
		for id, state := range owner.leases {
			if id == keep {
				continue
			}
			entry := retiredLease{owner: owner, id: id, key: key, state: state}
			if pending := owner.pending[id]; pending != nil && pending.restoreOnRetire {
				entry.restore = pending
				entry.targets = owner.rollbackTargets(pending)
			}
			if !retryOnly || state == candidateRevokePending || entry.restore != nil {
				retired = append(retired, entry)
				if keep == 0 && id == r.leaseID {
					// Close handles the current lease before starting the pool.
					last := len(retired) - 1
					retired[0], retired[last] = retired[last], retired[0]
				}
			}
		}
	}
	if len(retired) == 0 {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(parent, revokeTimeout)
	defer cancel()
	start := 0
	if keep == 0 && retired[0].id == r.leaseID {
		// Complete the current lease's RPC before candidates can use the transport.
		entry := &retired[0]
		entry.err = r.revokeLease(ctx, entry.id)
		entry.revoked = entry.err == nil
		if entry.err != nil {
			entry.err = fmt.Errorf("revoke etcd lease %d: %w", entry.id, entry.err)
		}
		start = 1
	}
	var workers sync.WaitGroup
	const parallelRevocations = 8
	workerCount := min(len(retired)-start, parallelRevocations)
	for worker := range workerCount {
		workers.Go(func() {
			for i := start + worker; i < len(retired) && ctx.Err() == nil; i += workerCount {
				entry := &retired[i]
				if entry.restore != nil {
					attempt := *entry.restore
					unresolved, err := r.resolveUnconfirmed(ctx, entry.key, &attempt, entry.targets, nil)
					entry.settled, entry.err = !unresolved, err
					if unresolved {
						entry.err = fmt.Errorf("restore etcd key %q before revoking lease %d: %w", entry.key, entry.id, err)
						continue
					}
				} else {
					entry.err = r.revokeLease(ctx, entry.id)
				}
				entry.revoked = entry.err == nil
				if entry.err != nil {
					entry.err = fmt.Errorf("revoke etcd lease %d: %w", entry.id, entry.err)
				}
			}
		})
	}
	workers.Wait()
	var failures []error
	ledgerChanged := false
	for _, entry := range retired {
		canRevoke := entry.restore == nil || entry.settled
		if _, pending := entry.owner.pending[entry.id]; pending && canRevoke {
			delete(entry.owner.pending, entry.id)
			ledgerChanged = true
		}
		if entry.revoked {
			delete(entry.owner.leases, entry.id)
			ledgerChanged = true
			if entry.owner.emptyKeyCandidate == entry.id {
				entry.owner.emptyKeyCandidate = 0
			}
			continue
		}
		if entry.err == nil {
			entry.err = fmt.Errorf("clean up etcd lease %d: %w", entry.id, ctx.Err())
		}
		failures = append(failures, entry.err)
		if entry.state != confirmedLease {
			cleanupPending = true
			if canRevoke && entry.state != candidateRevokePending {
				entry.owner.leases[entry.id] = candidateRevokePending
				ledgerChanged = true
			}
		}
	}
	if ledgerChanged {
		// A concurrent Get may still contain a lease that was just retired.
		r.ownershipVersion++
	}
	return errors.Join(failures...), cleanupPending
}

func (r *Register) queueCandidateRevoke(ownership *registrationOwnership, leaseID clientv3.LeaseID) {
	ownership.leases[leaseID] = candidateRevokePending
	r.startCleanupRetryLocked()
}

func (r *Register) startCleanupRetryLocked() {
	if !r.cleanupRetryRunning && r.ctx.Err() == nil {
		r.cleanupRetryRunning = true
		go r.retryCandidateCleanup()
	}
}

func (r *Register) retryCandidateCleanup() {
	for delay := initialRetryDelay; ; delay = min(delay*2, maxRetryDelay) {
		timer := time.NewTimer(delay)
		select {
		case <-r.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		r.mu.Lock()
		if r.ctx.Err() == nil {
			if _, pending := r.revokeRetiredLeases(r.ctx, r.leaseID, true); pending {
				r.mu.Unlock()
				continue
			}
		}
		r.cleanupRetryRunning = false
		r.mu.Unlock()
		return
	}
}

func (r *Register) IsAlive() bool {
	health := r.health.Load()
	return health != nil && health.registered.Load() && health.keepAliveCtx.Err() == nil && r.ctx.Err() == nil
}

func (r *Register) LeaseID() clientv3.LeaseID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaseID
}

// Close 注销服务并释放资源，返回注册恢复、租约撤销或客户端关闭失败的错误。
func (r *Register) Close() error {
	// 先取消正在执行的 Grant/Put，再等待注册释放锁。
	r.cancel()
	r.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), revokeTimeout)
		defer cancel()
		r.mu.Lock()
		defer r.mu.Unlock()

		key := r.registeredKey
		if key == "" {
			key = r.lastAttemptKey
		}
		if key == "" {
			key = r.opt.Key
		}
		log.Info("正在注销服务: %s", key)
		r.stopKeepAliveLocked()
		r.recovering = nil
		leaseErr, _ := r.revokeRetiredLeases(ctx, 0, false)
		r.leaseID = 0
		r.closeErr = r.client.Close()
		if leaseErr != nil {
			r.closeErr = errors.Join(leaseErr, r.closeErr)
		}
		if r.closeErr != nil {
			log.Error("服务注销失败: key=%s, err=%v", key, r.closeErr)
		} else {
			log.Info("服务注销完成: %s", key)
		}
	})
	return r.closeErr
}
