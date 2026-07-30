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
// 中断后注册器会尝试重新注册；自动恢复失败时调用 OnLeaseLost。
//
// IsAlive 只表示本地注册器当前认为租约有效，不能代替业务侧的端到端健康检查。
// LeaseID 可用于诊断。ReRegister 会撤销旧租约并重新执行注册。Close 应在进程
// 退出时调用，它会取消保活、尽力撤销租约并关闭 etcd 客户端。
package register

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chenyao-lib/go/log"

	clientv3 "go.etcd.io/etcd/client/v3"
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
// DialTimeout 和 GrantTimeout 默认均为 5 秒。
// AutoReRegister 默认为 true，租约丢失后自动重新注册。
// OnLeaseLost 在自动重注册失败时触发（AutoReRegister=true），
// 或 keepalive 断开时直接触发（AutoReRegister=false）。
type RegisterOption struct {
	Key   string // etcd key，与 KeyFunc 互斥
	Value string // etcd value，与 ValueFunc 互斥

	KeyFunc   func() string // 动态生成 key（优先级高于 Key）
	ValueFunc func() string // 动态生成 value（优先级高于 Value）

	TTL int // 租约 TTL（秒），默认 10

	// DialTimeout etcd 客户端连接超时，默认 5 秒。
	DialTimeout time.Duration

	// GrantTimeout 租约创建超时，默认 5 秒。
	GrantTimeout time.Duration

	// AutoReRegister 租约丢失后是否自动重新注册。
	// 默认 true，会在 keepalive 断开后自动尝试 ReRegister()。
	// 设为 false 则仅触发 OnLeaseLost 回调。
	AutoReRegister bool

	// OnLeaseLost 租约丢失回调。
	//   - AutoReRegister=true ：自动重注册失败时触发，err 为重注册错误
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
	o.AutoReRegister = true
}

// ---------------------------------------------------------------------------
// Register - 通用 etcd 注册器
// ---------------------------------------------------------------------------

// Register 通用 etcd 服务注册器。
type Register struct {
	client      *clientv3.Client
	opt         RegisterOption
	leaseID     clientv3.LeaseID
	keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	registered  bool
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
		log.Error("[ETCD-REGISTER] 创建 etcd 客户端失败: endpoints=%s, err=%v", endpoints, err)
		return nil, fmt.Errorf("etcd client error: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	reg := &Register{
		client: cli,
		opt:    opt,
		ctx:    ctx,
		cancel: cancel,
	}
	log.Info(
		"[ETCD-REGISTER] 注册器创建成功: endpoints=%s, key=%s, ttl=%ds",
		endpoints,
		opt.getKey(),
		opt.TTL,
	)
	return reg, nil
}

// Register 执行首次注册。
// 租约创建 → Put key/value → 启动 keepalive，全过程同步返回。
func (r *Register) Register() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.doRegister(); err != nil {
		log.Error("[ETCD-REGISTER] 服务注册失败: key=%s, err=%v", r.opt.getKey(), err)
		return err
	}
	return nil
}

// doRegister 内部注册逻辑（无锁）
func (r *Register) doRegister() error {
	key := r.opt.getKey()
	value := r.opt.getValue()

	leaseCtx, cancel := context.WithTimeout(r.ctx, r.opt.GrantTimeout)
	leaseResp, err := r.client.Grant(leaseCtx, int64(r.opt.TTL))
	cancel()
	if err != nil {
		return fmt.Errorf("etcd grant lease error: %w", err)
	}

	r.leaseID = leaseResp.ID

	_, err = r.client.Put(r.ctx, key, value, clientv3.WithLease(r.leaseID))
	if err != nil {
		return fmt.Errorf("etcd put error: %w", err)
	}

	r.keepAliveCh, err = r.client.KeepAlive(r.ctx, r.leaseID)
	if err != nil {
		return fmt.Errorf("etcd keepalive error: %w", err)
	}

	r.registered = true
	log.Info("[ETCD] 服务注册成功: %s = %s (TTL=%ds)", key, value, r.opt.TTL)

	go r.watchKeepAlive()
	return nil
}

// watchKeepAlive 监听 keepalive 响应。
// 通道关闭时根据 AutoReRegister 决定是否自动重注册。
func (r *Register) watchKeepAlive() {
	for {
		select {
		case _, ok := <-r.keepAliveCh:
			if !ok {
				log.Error("[ETCD] keepalive 通道关闭，服务注册可能已失效: %s", r.opt.getKey())

				r.mu.Lock()
				r.registered = false
				autoReReg := r.opt.AutoReRegister
				onLost := r.opt.OnLeaseLost
				r.mu.Unlock()

				if autoReReg {
					log.Info("[ETCD] 正在自动重新注册: %s", r.opt.getKey())
					if err := r.ReRegister(); err != nil {
						log.Error("[ETCD] 自动重注册失败: %v", err)
						if onLost != nil {
							onLost(err)
						}
					} else {
						log.Info("[ETCD] 自动重注册成功: %s", r.opt.getKey())
					}
				} else {
					if onLost != nil {
						onLost(nil)
					}
				}
				return
			}

		case <-r.ctx.Done():
			log.Info("[ETCD] 保活监听正常退出: %s", r.opt.getKey())
			return
		}
	}
}

// ReRegister 重新注册。会先撤销旧租约，然后重新注册。
func (r *Register) ReRegister() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.ctx.Done():
		return errors.New("register is closed")
	default:
	}

	if r.registered && r.leaseID != 0 {
		_, err := r.client.Revoke(context.Background(), r.leaseID)
		if err != nil {
			log.Error("[ETCD] 撤销旧租约失败: %v", err)
		}
	}

	if err := r.doRegister(); err != nil {
		log.Error("[ETCD-REGISTER] 服务重新注册失败: key=%s, err=%v", r.opt.getKey(), err)
		return err
	}
	return nil
}

// IsAlive 返回当前注册是否有效。
func (r *Register) IsAlive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.registered
}

// LeaseID 返回当前租约 ID。
func (r *Register) LeaseID() clientv3.LeaseID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaseID
}

// Close 注销服务并释放资源。
func (r *Register) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Info("[ETCD] 正在注销服务: %s", r.opt.getKey())

	r.cancel()

	if r.registered && r.leaseID != 0 {
		_, err := r.client.Revoke(context.Background(), r.leaseID)
		if err != nil {
			log.Error("[ETCD] 撤销租约失败: %v", err)
		}
		r.registered = false
	}

	if err := r.client.Close(); err != nil {
		log.Error("[ETCD-REGISTER] 关闭 etcd 客户端失败: key=%s, err=%v", r.opt.getKey(), err)
		return err
	}
	log.Info("[ETCD-REGISTER] 服务注销完成: key=%s", r.opt.getKey())
	return nil
}
