// Package nodeaddr 提供服务节点地址的唯一构造与解析入口。
//
// 服务发现（etcd 注册/监听）、token 签发、WebSocket 拨号等处都用 "host:port"
// 字符串传递节点地址。过去各处自行 fmt.Sprintf("%s:%d") 拼装、自行切分字符串，
// 一旦出现 IPv6、主机名、多余空白或解析失败的 etcd value，就会得到彼此不相等的
// 字符串，导致「加入环的地址」和「删除时用的地址」对不上，留下幽灵节点。
//
// 本包把这件事收敛到一处：服务端内部逻辑用 NodeAddr，跨进程边界（etcd value、
// token.game_addr、ws:// 拨号串）统一走 String() 出、ParseNodeAddr() 入。
// 注意协议字段本身仍是 string（如 token.LoginToken.GameAddr），本包只保证
// 这个 string 是唯一且可校验地产生/消费的，不改变线上协议格式。
package nodeaddr

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

// NodeAddr 一个服务节点的可拨号地址。
type NodeAddr struct {
	Host string // IP 或主机名；IPv6 字面量不带方括号
	Port int    // 1..65535
}

// Validate 校验该地址能否安全地拼装成跨进程传递的 "host:port"。
// 需要处理校验错误的构造方应先调用本方法，再调用 String。
func (a NodeAddr) Validate() error {
	_, err := a.normalized()
	return err
}

func (a NodeAddr) normalized() (NodeAddr, error) {
	host, err := normalizeHost(a.Host)
	if err != nil {
		return NodeAddr{}, err
	}
	if a.Port < 1 || a.Port > 65535 {
		return NodeAddr{}, fmt.Errorf("nodeaddr: invalid port %d", a.Port)
	}
	a.Host = host
	return a, nil
}

func normalizeHost(host string) (string, error) {
	if host == "" {
		return "", errors.New("nodeaddr: empty host")
	}
	// Host 里出现这些字符说明传入的其实是 URL / etcd key / JSON 片段 / 带括号的
	// IPv6 字面量，拼进 JoinHostPort 后会得到无法解析的地址。
	if strings.ContainsAny(host, " \t\r\n/{}[]\"@?#\\") {
		return "", fmt.Errorf("nodeaddr: invalid host %q", host)
	}
	if strings.Contains(host, ":") {
		addr, err := netip.ParseAddr(host)
		if err != nil {
			return "", fmt.Errorf("nodeaddr: invalid IPv6 host %q: %w", host, err)
		}
		return addr.String(), nil
	}
	return host, nil
}

// String 是唯一的地址拼装点。
// IPv6 字面量会统一写法并补方括号，zone 随地址保留。
func (a NodeAddr) String() string {
	host := a.Host
	if normalized, err := normalizeHost(host); err == nil {
		host = normalized
	}
	return net.JoinHostPort(host, strconv.Itoa(a.Port))
}

// ParseNodeAddr 是唯一的地址解析点。
// 接受 "10.0.0.8:8080"、"game-svc-1:8080"、"[::1]:8080"；
// 拒绝空 host、非法/越界端口、带 scheme 或路径的串、JSON 片段等脏值。
// 返回的 NodeAddr 已通过 Validate，可直接 String() 归一化。
func ParseNodeAddr(s string) (NodeAddr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return NodeAddr{}, errors.New("nodeaddr: empty addr")
	}
	// 预检：挡掉 URL / etcd key / JSON 片段等明显不是 host:port 的串，
	// 避免解析失败后把整串原样当成地址塞进哈希环、token 和 ws:// 拨号。
	if strings.Contains(s, "/") || strings.ContainsAny(s, " \t\r\n{}\"") {
		return NodeAddr{}, fmt.Errorf("nodeaddr: invalid addr %q", s)
	}
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return NodeAddr{}, fmt.Errorf("nodeaddr: invalid addr %q: %w", s, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return NodeAddr{}, fmt.Errorf("nodeaddr: invalid port %q in %q", portStr, s)
	}
	return (NodeAddr{Host: host, Port: port}).normalized()
}
