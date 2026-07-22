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

// GetNode 轮询选择节点
// key 参数在轮询策略中忽略
func (s *RoundRobinStrategy) GetNode(_ string) string {
	s.mu.RLock()
	n := len(s.nodes)
	s.mu.RUnlock()
	if n == 0 {
		return ""
	}
	idx := s.index.Add(1) % int32(n)
	s.mu.RLock()
	addr := s.nodes[idx]
	s.mu.RUnlock()
	return addr
}

func (s *RoundRobinStrategy) Nodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, len(s.nodes))
	copy(result, s.nodes)
	return result
}
