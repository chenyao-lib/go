// 一致性哈希环实现
package watcher

import (
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// ConsistentHash 一致性哈希环
// 实现了 SelectStrategy 接口
type ConsistentHash struct {
	nodes      []uint64
	nodeMap    map[uint64]string
	virtualNum int
	mu         sync.RWMutex
}

// NewConsistentHash 创建一致性哈希环
// virtualNum 为每个真实节点对应的虚拟节点数量
func NewConsistentHash(virtualNum int) *ConsistentHash {
	return &ConsistentHash{
		nodeMap:    make(map[uint64]string),
		virtualNum: virtualNum,
	}
}

func (ch *ConsistentHash) AddNode(addr string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	// 去重：同一物理节点只入环一次（watch 重连全量同步后重放的 Put 事件可能重复 Add）
	for _, a := range ch.nodeMap {
		if a == addr {
			return
		}
	}
	for i := 0; i < ch.virtualNum; i++ {
		hashKey := xxhash.Sum64String(addr + "#" + string(rune(i)))
		ch.nodes = append(ch.nodes, hashKey)
		ch.nodeMap[hashKey] = addr
	}
	sort.Slice(ch.nodes, func(i, j int) bool { return ch.nodes[i] < ch.nodes[j] })
}

func (ch *ConsistentHash) RemoveNode(addr string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for i := 0; i < ch.virtualNum; i++ {
		hashKey := xxhash.Sum64String(addr + "#" + string(rune(i)))
		delete(ch.nodeMap, hashKey)
	}
	newNodes := make([]uint64, 0, len(ch.nodes))
	for _, h := range ch.nodes {
		if _, ok := ch.nodeMap[h]; ok {
			newNodes = append(newNodes, h)
		}
	}
	ch.nodes = newNodes
}

// Reset 清空所有节点（watch 断线重连后的全量重新同步使用）
func (ch *ConsistentHash) Reset() {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.nodes = ch.nodes[:0]
	ch.nodeMap = make(map[uint64]string)
}

// GetNode 根据 key 在哈希环上查找目标节点
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	if len(ch.nodes) == 0 {
		return ""
	}
	hash := xxhash.Sum64String(key)
	idx := sort.Search(len(ch.nodes), func(i int) bool { return ch.nodes[i] >= hash })
	if idx == len(ch.nodes) {
		idx = 0
	}
	return ch.nodeMap[ch.nodes[idx]]
}

func (ch *ConsistentHash) Nodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	addrSet := make(map[string]struct{}, len(ch.nodes))
	for _, addr := range ch.nodeMap {
		addrSet[addr] = struct{}{}
	}
	result := make([]string, 0, len(addrSet))
	for addr := range addrSet {
		result = append(result, addr)
	}
	return result
}
