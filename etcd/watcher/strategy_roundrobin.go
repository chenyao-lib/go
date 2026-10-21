// 轮询策略实现
package watcher

import (
	"sync"
	"sync/atomic"
)

// RoundRobinStrategy 轮询策略
type RoundRobinStrategy struct {
	nodes []string
	index atomic.Int32
	mu    sync.RWMutex
}

// NewRoundRobinStrategy 创建轮询策略
func NewRoundRobinStrategy() *RoundRobinStrategy {
	return &RoundRobinStrategy{
		nodes: make([]string, 0),
	}
}

func (s *RoundRobinStrategy) AddNode(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.nodes {
		if v == addr {
			return
		}
	}
	s.nodes = append(s.nodes, addr)
}

func (s *RoundRobinStrategy) RemoveNode(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, v := range s.nodes {
		if v == addr {
			s.nodes = append(s.nodes[:i], s.nodes[i+1:]...)
			return
		}
	}
}

// Reset 清空所有节点（watch 断线重连后的全量重新同步使用）
func (s *RoundRobinStrategy) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = s.nodes[:0]
	s.index.Store(0)
}

// GetNode 轮询选择节点
// key 参数在轮询策略中忽略
func (s *RoundRobinStrategy) GetNode(_ string) string {
	// 取长度与取节点必须在同一把读锁内完成：旧实现分两次 RLock，
	// 中间若 RemoveNode 让节点数变少，s.nodes[idx] 会越界 panic。
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.nodes)
	if n == 0 {
		return ""
	}
	idx := int(s.index.Add(1)) % n
	return s.nodes[idx]
}

func (s *RoundRobinStrategy) Nodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, len(s.nodes))
	copy(result, s.nodes)
	return result
}
