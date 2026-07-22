// 节点选择策略接口定义
package watcher

// SelectStrategy 节点选择策略接口
type SelectStrategy interface {
	// AddNode 添加节点
	AddNode(addr string)
	// RemoveNode 移除节点
	RemoveNode(addr string)
	// GetNode 根据 key 获取目标节点
	// key 为空时按策略默认方式选择（如轮询）
	GetNode(key string) string
	// Nodes 返回所有节点列表
	Nodes() []string
}
