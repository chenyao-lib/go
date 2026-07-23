package srvsvcbinding

import (
	"context"
	"errors"
	"sync"

	"github.com/chenyao-lib/go/srv"
	"github.com/chenyao-lib/go/svc"

	"github.com/chenyao-lib/go/log"
)

type ClientServiceBinding struct {
	services   sync.Map
	svcFactory func(c *srv.Client) svc.Instance
}

func NewClientServiceBinding(ws *srv.WsServer, svcFactory func(c *srv.Client) svc.Instance) *ClientServiceBinding {
	b := &ClientServiceBinding{svcFactory: svcFactory}
	ws.SetOnClientReady(b.onClientReady)
	ws.SetOnClientClose(b.onClientClose)
	ws.RegisterDefaultHandler(b.handleMessage)
	return b
}

func (b *ClientServiceBinding) onClientReady(c *srv.Client) {
	connSvc := b.svcFactory(c)
	if err := connSvc.Start(); err != nil {
		log.Error("start connection service failed: %v", err)
		c.CloseWithReason(1011, "start connection service failed")
		return
	}
	b.services.Store(c, connSvc)
	log.Info("connection service started: %s", connSvc.Name())
}

func (b *ClientServiceBinding) onClientClose(c *srv.Client) {
	if v, ok := b.services.LoadAndDelete(c); ok {
		connSvc := v.(svc.Instance)
		connSvc.Stop()
		log.Info("connection service stopped: %s", connSvc.Name())
	}
}

func (b *ClientServiceBinding) handleMessage(c *srv.Client, msg srv.RPCMessage) (any, error) {
	v, ok := b.services.Load(c)
	if !ok {
		return nil, errors.New("connection service not found")
	}
	connSvc := v.(svc.Instance)

	if msg.Session != "" {
		return connSvc.Request(context.Background(), msg.Method, msg.Data)
	}
	return nil, connSvc.Post(context.Background(), msg.Method, msg.Data)
}
