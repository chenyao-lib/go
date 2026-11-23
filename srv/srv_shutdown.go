package srv

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/chenyao-lib/go/log"
	"github.com/coder/websocket"
)

const defaultShutdownGracePeriod = 5 * time.Second

// SetShutdownGracePeriod 设置 WebSocket 关闭握手的共同宽限期，默认 5 秒。
// 在 Start 前调用。0 表示立即强制关闭，负值会被忽略。
func (s *WsServer) SetShutdownGracePeriod(period time.Duration) {
	if period >= 0 {
		s.shutdownGrace = period
	}
}

// Shutdown 停止接入，并等待已接入连接的读写、业务处理和生命周期回调退出。
// 宽限期或 ctx 到期时会断开连接，若 ctx 先于清理完成到期则返回 ctx.Err()。
// 宽限期不约束业务 Handler：它们应通过连接 Context 的取消信号退出，否则收尾会持续等待。
// 在 Start 之前调用会直接进入终态，之后的 Start 返回错误。
func (s *WsServer) Shutdown(ctx context.Context) error {
	s.lifecycleMu.Lock()
	httpServer := s.httpServer
	if !s.stopping {
		s.stopping = true
		if httpServer == nil {
			close(s.shutdownDone)
			s.lifecycleMu.Unlock()
			return nil
		}
		go s.drainConnections(httpServer)
	}
	s.lifecycleMu.Unlock()

	select {
	case <-s.shutdownDone:
		return s.shutdownErr
	default:
	}
	select {
	case <-s.shutdownDone:
		return s.shutdownErr
	case <-ctx.Done():
		if httpServer != nil {
			s.forceShutdown(httpServer)
		}
		return ctx.Err()
	}
}

func (s *WsServer) drainConnections(httpServer *http.Server) {
	defer close(s.shutdownDone)
	grace, cancel := context.WithTimeout(context.Background(), s.shutdownGrace)
	defer cancel()

	if s.shutdownGrace > 0 {
		for _, client := range s.activeConnections() {
			go client.CloseWithReason(websocket.StatusGoingAway, "server shutdown")
		}
	}

	connectionsDone := make(chan struct{})
	go func() {
		s.connectionsWG.Wait()
		close(connectionsDone)
	}()

	err := httpServer.Shutdown(grace)
	if err == nil {
		select {
		case <-connectionsDone:
			return
		case <-grace.Done():
		}
	} else if !errors.Is(err, context.DeadlineExceeded) {
		s.shutdownErr = err
	}

	s.forceShutdown(httpServer)
	<-connectionsDone
}

func (s *WsServer) forceShutdown(httpServer *http.Server) {
	s.forceOnce.Do(func() {
		for _, client := range s.activeConnections() {
			client.forceClose()
		}
		if err := httpServer.Close(); err != nil {
			log.Warn("强制关闭 HTTP 服务失败: %v", err)
		}
	})
}

func (s *WsServer) activeConnections() []*Client {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	clients := make([]*Client, 0, len(s.connections))
	for client := range s.connections {
		clients = append(clients, client)
	}
	return clients
}

func (s *WsServer) finishConnection(c *Client) {
	<-c.closeDone
	s.lifecycleMu.Lock()
	delete(s.connections, c)
	s.lifecycleMu.Unlock()
	s.connectionsWG.Done()
}

func (c *Client) forceClose() {
	// websocket.CloseNow 会等待已开始的关闭握手。直接关闭传输层才能中断它。
	_ = c.transport.Close()
	c.cancel()
	c.closeNow()
}

// websocketResponseWriter 保留升级后的连接，用于在关停宽限期结束时断开传输层。
type websocketResponseWriter struct {
	http.ResponseWriter
	transport net.Conn
}

func (w *websocketResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, brw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.transport = conn
	}
	return conn, brw, err
}
