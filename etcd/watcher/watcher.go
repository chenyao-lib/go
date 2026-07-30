// Package watcher 从 etcd 前缀加载并持续监听服务节点，并通过轮询、一致性哈希
// 或调用方自定义的 SelectStrategy 选择节点。
//
// # 创建独立 Watcher
//
//	nodes, err := watcher.NewWatcher(
//		"127.0.0.1:2379",
//		"services/game/",
//		10,
//		watcher.WithHashRing(150),
//	)
//	if err != nil {
//		return err
//	}
//	if err := nodes.Start(); err != nil {
//		return err
//	}
//	defer nodes.Close()
//
//	addr := nodes.GetNode(playerID)
//	if addr == "" {
//		return errors.New("no available game server")
//	}
//
// NewWatcher 默认使用轮询；WithRoundRobin 可显式选择轮询，WithHashRing 的参数
// 是每个真实节点的虚拟节点数，WithStrategy 可安装自定义并发安全策略。Start
// 会先建立 etcd 保活上下文、加载已有节点，再启动后台 watch。必须在成功 Start
// 后调用 GetNode，并在退出时 Close。
//
// # etcd 数据格式
//
// prefix 下每个 value 支持三种格式：
//
//	{"ip":"10.0.0.8","port":8080,"buildNo":"1.0.0"}
//	{"addr":"10.0.0.8:8080","weight":100}
//	10.0.0.8:8080
//
// 删除事件会从 key 中按 prefix 提取地址，因此注册端应保持 key 与 value 的地址
// 含义一致，例如 services/game/10.0.0.8:8080。Nodes 返回当前节点快照。
//
// 轮询策略忽略 GetNode 的 key；一致性哈希策略使用 key 保持同一业务标识尽量
// 落在同一节点。没有节点时两种策略都返回空字符串。
//
// # 默认实例
//
// 简单程序可调用 Init 创建包级 Default，然后用 watcher.GetNode。Init 支持
// HashRing 和 RoundRobin。需要多个前缀、不同策略或明确管理生命周期时，应使用
// NewWatcher；使用 Default 的程序退出时也应调用 watcher.Default.Close()。
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
	log.Info("connecting to etcd: %s", endpoints)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Error("创建 etcd 客户端失败: endpoints=%s, prefix=%s, err=%v", endpoints, prefix, err)
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
		log.Error("创建保活租约失败: prefix=%s, ttl=%ds, err=%v", nw.prefix, nw.leaseTTL, err)
		nw.cli.Close()
		return err
	}
	nw.ctx = ctx
	nw.cancel = cancel

	// 2. 加载已有 node（2s 超时，失败立即退出）
	getCtx, getCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer getCancel()
	if err := nw.loadNodes(getCtx); err != nil {
		log.Error("初始节点加载失败: prefix=%s, err=%v", nw.prefix, err)
		nw.cancel()
		nw.cli.Close()
		return err
	}

	// 3. 启动 watcher
	go nw.watchNodes()
	log.Info("节点发现启动成功: prefix=%s, strategy=%T, nodes=%d", nw.prefix, nw.strategy, len(nw.Nodes()))
	return nil
}

// Close 关闭节点发现器
func (nw *NodeWatcher) Close() error {
	var err error
	nw.closeOnce.Do(func() {
		log.Info("正在关闭节点发现: prefix=%s", nw.prefix)
		if nw.cancel != nil {
			nw.cancel()
		}
		err = nw.cli.Close()
		if err != nil {
			log.Error("关闭节点发现失败: prefix=%s, err=%v", nw.prefix, err)
		} else {
			log.Info("节点发现已关闭: prefix=%s", nw.prefix)
		}
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
	log.Info("etcd watch nodes started, prefix=%s", nw.prefix)

	watchChan := nw.cli.Watch(nw.ctx, nw.prefix, clientv3.WithPrefix())
	for wresp := range watchChan {
		if wresp.Err() != nil {
			log.Error("watch nodes error: %+v", wresp.Err())
			continue
		}
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				addr := parseNodeAddr(ev.Kv.Value)
				if addr != "" {
					nw.strategy.AddNode(addr)
					log.Info("Added node: %s", addr)
				} else {
					log.Error("parse node addr failed, key=%s value=%s", string(ev.Kv.Key), string(ev.Kv.Value))
				}
			case clientv3.EventTypeDelete:
				addr := extractAddrFromKey(string(ev.Kv.Key), nw.prefix)
				nw.strategy.RemoveNode(addr)
				log.Info("Removed node: %s", addr)
			}
		}
	}
	if nw.ctx != nil && nw.ctx.Err() != nil {
		log.Info("节点监听正常退出: prefix=%s", nw.prefix)
	} else {
		log.Warn("节点监听意外退出: prefix=%s", nw.prefix)
	}
}

// createEtcdLease 创建租约并持续 keepalive，返回可取消的 context
func createEtcdLease(cli *clientv3.Client, leaseTTL int) (context.Context, context.CancelFunc, error) {
	log.Info("creating etcd lease (TTL %ds)...", leaseTTL)

	leaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	lease, err := cli.Grant(leaseCtx, int64(leaseTTL))
	cancel()
	if err != nil {
		return nil, nil, errors.New("etcd grant lease error: " + err.Error())
	}
	log.Info("etcd lease granted: %d", lease.ID)

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
		log.Error("etcd keepalive stopped")
		cancel()
	}()

	return ctx, cancel, nil
}

// loadNodes 从 etcd 加载已有 node 并写入策略
func (nw *NodeWatcher) loadNodes(ctx context.Context) error {
	log.Info("loading existing nodes from etcd...")

	resp, err := nw.cli.Get(ctx, nw.prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.New("etcd get nodes error: " + err.Error())
	}

	for _, kv := range resp.Kvs {
		addr := parseNodeAddr(kv.Value)
		if addr != "" {
			nw.strategy.AddNode(addr)
			log.Info("Loaded node: %s", addr)
		} else {
			log.Error("parse node addr failed, key=%s value=%s", string(kv.Key), string(kv.Value))
		}
	}
	log.Info("loaded %d nodes", len(resp.Kvs))
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
