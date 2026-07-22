// 节点发现核心：etcd 连接管理、节点加载与监听
package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chenyao-lib/go/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// GameServer 兼容 addr/weight 注册格式
type GameServer struct {
	Addr   string `json:"addr"`
	Weight int    `json:"weight"`
}

// ServerInfo 兼容 ip/port 注册格式:
// {"ip":"192.168.3.118","port":54335,"buildNo":"1,0,0,0"}
type ServerInfo struct {
	IP      string `json:"ip"`
	Port    int    `json:"port"`
	BuildNo string `json:"buildNo"`
}

// NodeWatcher 通用节点发现器
// 监听 etcd 特定 prefix 下的节点变化，将节点列表同步到指定的 SelectStrategy 中
type NodeWatcher struct {
	cli       *clientv3.Client
	prefix    string
	leaseTTL  int
	strategy  SelectStrategy
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// NodeWatcherOption 函数式选项
type NodeWatcherOption func(*NodeWatcher)

// WithHashRing 使用一致性哈希环策略
func WithHashRing(virtualNodes int) NodeWatcherOption {
	return func(nw *NodeWatcher) {
		nw.strategy = NewConsistentHash(virtualNodes)
	}
}

// WithRoundRobin 使用轮询策略（默认）
func WithRoundRobin() NodeWatcherOption {
	return func(nw *NodeWatcher) {
		nw.strategy = NewRoundRobinStrategy()
	}
}

// WithStrategy 使用自定义策略
func WithStrategy(s SelectStrategy) NodeWatcherOption {
	return func(nw *NodeWatcher) {
		nw.strategy = s
	}
}

// NewWatcher 创建节点发现器
func NewWatcher(endpoints, prefix string, leaseTTL int, opts ...NodeWatcherOption) (*NodeWatcher, error) {
	log.Info("[WATCHER] connecting to etcd: %s", endpoints)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, errors.New("etcd client error: " + err.Error())
	}

	nw := &NodeWatcher{
		cli:      cli,
		prefix:   prefix,
		leaseTTL: leaseTTL,
		// 默认使用轮询策略
		strategy: NewRoundRobinStrategy(),
	}

	for _, opt := range opts {
		opt(nw)
	}

	return nw, nil
}

// Start 启动节点发现：
//  1. 创建租约保活
//  2. 加载已有 node
//  3. 启动 watch
func (nw *NodeWatcher) Start() error {
	// 1. 创建租约保活
	ctx, cancel, err := createEtcdLease(nw.cli, nw.leaseTTL)
	if err != nil {
		nw.cli.Close()
		return err
	}
	nw.ctx = ctx
	nw.cancel = cancel

	// 2. 加载已有 node（2s 超时，失败立即退出）
	getCtx, getCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer getCancel()
	if err := nw.loadNodes(getCtx); err != nil {
		nw.cli.Close()
		return err
	}

	// 3. 启动 watcher
	go nw.watchNodes()
	return nil
}

// Close 关闭节点发现器
func (nw *NodeWatcher) Close() error {
	var err error
	nw.closeOnce.Do(func() {
		if nw.cancel != nil {
			nw.cancel()
		}
		err = nw.cli.Close()
	})
	return err
}

// GetNode 根据 key 获取目标节点
func (nw *NodeWatcher) GetNode(key string) string {
	return nw.strategy.GetNode(key)
}

// Nodes 返回所有节点列表
func (nw *NodeWatcher) Nodes() []string {
	return nw.strategy.Nodes()
}

// parseNodeAddr 从 etcd value 解析 node 地址
// 兼容三种格式:
//  1. {"ip":"192.168.3.118","port":54335,"buildNo":"1,0,0,0"}
//  2. {"addr":"192.168.1.1:8080","weight":100}
//  3. 纯字符串: 192.168.1.1:8080
func parseNodeAddr(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	// 尝试 ip/port 格式
	var info ServerInfo
	if err := json.Unmarshal(value, &info); err == nil && info.IP != "" && info.Port > 0 {
		return fmt.Sprintf("%s:%d", info.IP, info.Port)
	}
	// 尝试 addr 格式
	var gs GameServer
	if err := json.Unmarshal(value, &gs); err == nil && gs.Addr != "" {
		return gs.Addr
	}
	// 回退：将 value 直接作为地址字符串
	addr := strings.TrimSpace(string(value))
	return addr
}

// extractAddrFromKey 从 etcd key 中提取 node 地址
// key 格式: {ServicePrefix}/{addr}  例如: fishserver/192.168.3.118:54335
func extractAddrFromKey(key, serverPrefix string) string {
	prefix := serverPrefix + "/"
	if addr := strings.TrimPrefix(key, prefix); addr != key {
		return addr
	}
	return strings.TrimPrefix(key, serverPrefix)
}

// watchNodes 监听 etcd 中 node 的增删变化
func (nw *NodeWatcher) watchNodes() {
	log.Info("[WATCHER] etcd watch nodes started, prefix=%s", nw.prefix)

	watchChan := nw.cli.Watch(nw.ctx, nw.prefix, clientv3.WithPrefix())
	for wresp := range watchChan {
		if wresp.Err() != nil {
			log.Error("[WATCHER] watch nodes error: %+v", wresp.Err())
			continue
		}
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				addr := parseNodeAddr(ev.Kv.Value)
				if addr != "" {
					nw.strategy.AddNode(addr)
					log.Info("[WATCHER] Added node: %s", addr)
				} else {
					log.Error("[WATCHER] parse node addr failed, key=%s value=%s", string(ev.Kv.Key), string(ev.Kv.Value))
				}
			case clientv3.EventTypeDelete:
				addr := extractAddrFromKey(string(ev.Kv.Key), nw.prefix)
				nw.strategy.RemoveNode(addr)
				log.Info("[WATCHER] Removed node: %s", addr)
			}
		}
	}
	log.Warn("[WATCHER] watchNodes exited")
}

// createEtcdLease 创建租约并持续 keepalive，返回可取消的 context
func createEtcdLease(cli *clientv3.Client, leaseTTL int) (context.Context, context.CancelFunc, error) {
	log.Info("[WATCHER] creating etcd lease (TTL %ds)...", leaseTTL)

	leaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	lease, err := cli.Grant(leaseCtx, int64(leaseTTL))
	cancel()
	if err != nil {
		return nil, nil, errors.New("etcd grant lease error: " + err.Error())
	}
	log.Info("[WATCHER] etcd lease granted: %d", lease.ID)

	ctx, cancel := context.WithCancel(context.Background())
	keepaliveCh, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		return nil, nil, errors.New("etcd keepalive error: " + err.Error())
	}

	go func() {
		// 消费 keepalive 通道防止阻塞，etcd client 内部自动续约
		for range keepaliveCh {
		}
		log.Error("[WATCHER] etcd keepalive stopped")
		cancel()
	}()

	return ctx, cancel, nil
}

// loadNodes 从 etcd 加载已有 node 并写入策略
func (nw *NodeWatcher) loadNodes(ctx context.Context) error {
	log.Info("[WATCHER] loading existing nodes from etcd...")

	resp, err := nw.cli.Get(ctx, nw.prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.New("etcd get nodes error: " + err.Error())
	}

	for _, kv := range resp.Kvs {
		addr := parseNodeAddr(kv.Value)
		if addr != "" {
			nw.strategy.AddNode(addr)
			log.Info("[WATCHER] Loaded node: %s", addr)
		} else {
			log.Error("[WATCHER] parse node addr failed, key=%s value=%s", string(kv.Key), string(kv.Value))
		}
	}
	log.Info("[WATCHER] loaded %d nodes", len(resp.Kvs))
	return nil
}

// ────────────────────────────────────────────
// 默认全局 watcher（简化使用）
// ────────────────────────────────────────────

// StrategyType 策略类型
type StrategyType int

const (
	// StrategyHashRing 一致性哈希环策略
	HashRing StrategyType = iota
	// StrategyRoundRobin 轮询策略
	RoundRobin
)

var Default *NodeWatcher

// Init 初始化默认全局 watcher
func Init(etcdEndpoints, serverPrefix string, leaseTTL int, strategyType StrategyType) error {
	var opt NodeWatcherOption
	switch strategyType {
	case HashRing:
		opt = WithHashRing(150)
	case RoundRobin:
		opt = WithRoundRobin()
	default:
		return errors.New("unknown strategy type")
	}

	nw, err := NewWatcher(etcdEndpoints, serverPrefix, leaseTTL, opt)
	if err != nil {
		return err
	}
	if err := nw.Start(); err != nil {
		return err
	}
	Default = nw
	return nil
}

// GetNode 使用默认 watcher 获取节点
func GetNode(key string) string {
	if Default != nil {
		return Default.GetNode(key)
	}
	return ""
}
