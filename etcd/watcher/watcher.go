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
// 会先加载已有节点，再启动后台 watch。必须在成功 Start 后调用 GetNode，并在
// 退出时 Close。leaseTTL 仅为兼容旧签名保留，watcher 不注册服务，也不会创建
// 租约（旧实现创建的租约从未绑定任何 key，只会产生无意义的保活开销）。
//
// # etcd 数据格式
//
// prefix 下每个 value 支持三种格式：
//
//	{"ip":"10.0.0.8","port":8080,"buildNo":"1.0.0"}
//	{"addr":"10.0.0.8:8080","weight":100}
//	10.0.0.8:8080
//
// 地址统一经 nodeaddr 包构造/解析，解析失败的 value 会被拒绝进入节点列表
// （只打错误日志），不会再被原样当成一个「地址」。
//
// watcher 内部维护 etcd key -> 地址 的配对：Put 与全量同步时记录，Delete 时用
// 记录下的同一地址移除节点。因此 key 不再必须等于地址，逻辑名 key（例如共享库
// etcd/register 官方示例里的 services/gateway/gw-01）也能被正确删除；仅当某次
// Delete 找不到缓存时才回退为按 prefix 从 key 截取地址并告警。Nodes 返回当前节点快照。
//
// 轮询策略忽略 GetNode 的 key；一致性哈希策略使用 key 保持同一业务标识尽量
// 落在同一节点。没有节点时两种策略都返回空字符串。
//
// watch 断开（etcd 重启、网络抖动等）会自动重连并全量重新同步节点列表，
// 断连期间错过的节点增删不会造成本地列表与 etcd 长期不一致。
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
	"strings"
	"sync"
	"time"

	"github.com/chenyao-lib/go/log"
	"github.com/chenyao-lib/go/nodeaddr"
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
	leaseTTL  int // 仅为兼容 NewWatcher 签名保留，watcher 不创建租约
	strategy  SelectStrategy
	ctx       context.Context // 生命周期 context，仅 Close 时取消（与租约解耦）
	cancel    context.CancelFunc
	startOnce sync.Once
	closeOnce sync.Once

	// mu 保护 keyAddrs，并保证「缓存变更 + 策略变更」成对生效。
	mu sync.Mutex
	// keyAddrs: etcd key -> 该 key 最近一次 Put / 全量同步解析出的地址。
	// Delete 事件据此用同一个 addr 移除节点，不再依赖 key 后缀恰好等于地址。
	keyAddrs map[string]string
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
		keyAddrs: make(map[string]string),
	}

	for _, opt := range opts {
		opt(nw)
	}

	return nw, nil
}

// Start 启动节点发现：
//  1. 加载已有 node
//  2. 启动 watch（断线自动重连 + 全量重新同步）
func (nw *NodeWatcher) Start() error {
	var startErr error
	// startOnce 防重入：重复 Start 会覆盖 ctx/cancel 并启动多个 watch 协程。
	nw.startOnce.Do(func() {
		// 1. 生命周期 context：仅 Close 时取消。
		// 旧实现把 watch 挂在租约保活的 context 上——etcd 重启/网络抖动导致
		// keepalive 停止时会 cancel 掉 watch，节点列表从此冻结（错过的增删
		// 永远补不上，哈希环里出现指向已死实例的僵尸节点）。现已解耦。
		nw.ctx, nw.cancel = context.WithCancel(context.Background())

		// 2. 加载已有 node（2s 超时，失败立即退出）
		getCtx, getCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer getCancel()
		if err := nw.loadNodes(getCtx); err != nil {
			log.Error("初始节点加载失败: prefix=%s, err=%v", nw.prefix, err)
			nw.cancel()
			nw.cli.Close()
			startErr = err
			return
		}

		// 3. 启动 watcher
		go nw.watchNodes()
		log.Info("节点发现启动成功: prefix=%s, strategy=%T, nodes=%d", nw.prefix, nw.strategy, len(nw.Nodes()))
	})
	return startErr
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

// parseNodeAddr 从 etcd value 解析 node 地址，是 value -> addr 的唯一入口。
// 兼容三种格式（见包注释），三种格式最终都经 nodeaddr 统一构造/归一化：
//  1. {"ip":"192.168.3.118","port":54335,"buildNo":"1,0,0,0"}
//  2. {"addr":"192.168.1.1:8080","weight":100}
//  3. 纯字符串 192.168.1.1:8080（也兼容被 json.Marshal 过的字符串字面量）
//
// 解析失败一律返回空串：旧实现会把整串 value 原样当地址返回，一段脏 JSON 因此
// 能进入哈希环、写进 token.game_addr、最终拼进 ws:// 拨号串才暴露。
func parseNodeAddr(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	// ip/port 格式
	var info ServerInfo
	if err := json.Unmarshal(value, &info); err == nil && info.IP != "" && info.Port > 0 {
		na := nodeaddr.NodeAddr{Host: info.IP, Port: info.Port}
		if err := na.Validate(); err != nil {
			return ""
		}
		return na.String()
	}
	// addr 格式
	var gs GameServer
	if err := json.Unmarshal(value, &gs); err == nil && gs.Addr != "" {
		na, err := nodeaddr.ParseNodeAddr(gs.Addr)
		if err != nil {
			return ""
		}
		return na.String()
	}
	// 纯地址串；兼容 "10.0.0.8:8080" 这种被 json.Marshal 成带引号的写法。
	raw := strings.TrimSpace(string(value))
	var unquoted string
	if err := json.Unmarshal(value, &unquoted); err == nil {
		raw = strings.TrimSpace(unquoted)
	}
	na, err := nodeaddr.ParseNodeAddr(raw)
	if err != nil {
		return ""
	}
	return na.String()
}

// extractAddrFromKey 从 etcd key 中提取 node 地址。
// 仅用作 Delete 事件缓存未命中时的兜底；正常路径由 keyAddrs 提供地址，
// 不再要求 key 格式必须是 {ServicePrefix}/{addr}。
func extractAddrFromKey(key, serverPrefix string) string {
	prefix := serverPrefix + "/"
	if addr := strings.TrimPrefix(key, prefix); addr != key {
		return addr
	}
	return strings.TrimPrefix(key, serverPrefix)
}

// watchNodes 监听 etcd 中 node 的增删变化。
// watch 断开（etcd 重启、网络抖动、watch 粒度重建等导致 channel 关闭）后
// 自动重连并全量重新同步节点——否则断连期间错过的增删会让本地列表永久冻结。
func (nw *NodeWatcher) watchNodes() {
	log.Info("etcd watch nodes started, prefix=%s", nw.prefix)

	const retryDelay = 2 * time.Second

	for nw.ctx.Err() == nil {
		watchChan := nw.cli.Watch(nw.ctx, nw.prefix, clientv3.WithPrefix())

		// 每轮都"先建 watch 再全量同步"：快照与 watch 建立之间的事件由
		// channel 缓冲补放，配合 AddNode 去重 / RemoveNode 幂等保证最终一致。
		// 若反过来先同步后建 watch，两步之间的事件会永久丢失。首轮也不例外
		// （Start 的初始 loadNodes 与这里建 watch 之间同样存在窗口）。
		// 同步失败时保留旧节点列表：先 Reset 再请求会在 etcd 短暂不可用时
		// 把策略清空，造成重连窗口内所有 GetNode 返回空、业务直接拒绝服务。
		syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := nw.loadNodes(syncCtx); err != nil {
			log.Error("watch 节点全量同步失败(保留现有节点，断线后将继续重试): prefix=%s, err=%v", nw.prefix, err)
		}
		syncCancel()

		for wresp := range watchChan {
			if wresp.Err() != nil {
				log.Error("watch nodes error: %+v", wresp.Err())
				continue
			}
			nw.applyEvents(wresp.Events)
		}

		// channel 关闭：要么正在 Close（ctx 已取消），要么 etcd 连接断开
		if nw.ctx.Err() != nil {
			break
		}
		log.Warn("etcd watch 断开, %s 后重连并全量同步节点: prefix=%s", retryDelay, nw.prefix)

		time.Sleep(retryDelay)
		if nw.ctx.Err() != nil {
			break
		}
	}

	if nw.ctx != nil && nw.ctx.Err() != nil {
		log.Info("节点监听正常退出: prefix=%s", nw.prefix)
	} else {
		log.Warn("节点监听意外退出: prefix=%s", nw.prefix)
	}
}

// applyEvents 把一批 watch 事件应用到节点策略。
// key -> addr 的配对由 watcher 自己维护：Put 时记录，Delete 时用同一个 addr 移除。
// 这样共享库 etcd/register 官方示例里的逻辑名 key（services/gateway/gw-01）
// 也能正确删除，不会再因为「从 key 截出来的字符串 != 从 value 解析出的地址」
// 而在哈希环里留下幽灵节点。
func (nw *NodeWatcher) applyEvents(events []*clientv3.Event) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for _, ev := range events {
		key := string(ev.Kv.Key)
		switch ev.Type {
		case clientv3.EventTypePut:
			addr := parseNodeAddr(ev.Kv.Value)
			if addr == "" {
				log.Error("parse node addr failed (keep last known node), key=%s value=%s", key, string(ev.Kv.Value))
				continue
			}
			if old, ok := nw.keyAddrs[key]; ok && old != addr {
				// 同一 key 的地址变了（运行时改 IP/端口、或 value 被改写）：
				// 先撤掉旧地址，否则它会作为幽灵节点永久留在环里。
				nw.removeAddrIfUnused(key, old)
				log.Info("node addr changed: key=%s old=%s new=%s", key, old, addr)
			}
			nw.keyAddrs[key] = addr
			nw.strategy.AddNode(addr)
			log.Info("Added node: %s", addr)
		case clientv3.EventTypeDelete:
			addr, ok := nw.keyAddrs[key]
			if ok {
				delete(nw.keyAddrs, key)
				if nw.removeAddrIfUnused(key, addr) {
					log.Info("Removed node: %s (key=%s)", addr, key)
				} else {
					log.Info("Removed key: %s, addr=%s still used by other keys", key, addr)
				}
				continue
			}
			// 缓存未命中：只收到 Delete 而没见过对应 Put（watch 从较新 revision
			// 起步、或 key 由外部写入）。退回 key 解析并明确告警。
			fallback := extractAddrFromKey(key, nw.prefix)
			nw.strategy.RemoveNode(fallback)
			log.Warn("delete event without cached addr, fallback to key: key=%s addr=%s (若 key 与 value 地址不一致可能删不掉)", key, fallback)
		}
	}
}

// removeAddrIfUnused 仅当没有其它 key 仍指向该地址时才从策略移除。
// 调用方必须持有 nw.mu。返回是否真的移除了节点。
func (nw *NodeWatcher) removeAddrIfUnused(excludeKey, addr string) bool {
	for k, a := range nw.keyAddrs {
		if k != excludeKey && a == addr {
			return false
		}
	}
	nw.strategy.RemoveNode(addr)
	return true
}

// nodeEntry 一次全量同步里 etcd key 与其解析地址的配对。
type nodeEntry struct {
	key  string
	addr string
}

// fetchNodes 从 etcd 拉取 prefix 下的全部 key -> addr 配对。
// 只有请求成功才返回结果；失败时调用方应保留策略中的现有节点。
func (nw *NodeWatcher) fetchNodes(ctx context.Context) ([]nodeEntry, error) {
	log.Info("loading existing nodes from etcd...")

	resp, err := nw.cli.Get(ctx, nw.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.New("etcd get nodes error: " + err.Error())
	}

	entries := make([]nodeEntry, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		addr := parseNodeAddr(kv.Value)
		if addr != "" {
			entries = append(entries, nodeEntry{key: string(kv.Key), addr: addr})
		} else {
			log.Error("parse node addr failed, key=%s value=%s", string(kv.Key), string(kv.Value))
		}
	}
	return entries, nil
}

// loadNodes 从 etcd 全量同步 node 到策略。
// 先请求成功再 Reset + 重建，避免 etcd 短暂不可用时把已有节点清空。
// 缓存与策略在同一把锁下重建，避免两者出现不一致的版本，导致后续 Delete
// 又用旧地址去删节点。
func (nw *NodeWatcher) loadNodes(ctx context.Context) error {
	entries, err := nw.fetchNodes(ctx)
	if err != nil {
		return err
	}

	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.keyAddrs = make(map[string]string, len(entries))
	nw.strategy.Reset()
	for _, e := range entries {
		nw.keyAddrs[e.key] = e.addr
		nw.strategy.AddNode(e.addr)
		log.Info("Loaded node: %s (key=%s)", e.addr, e.key)
	}
	log.Info("loaded %d nodes", len(entries))
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
