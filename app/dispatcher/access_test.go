package dispatcher

import (
	"context"
	"testing"

	"github.com/v2fly/v2ray-core/v5/common/log"
	"github.com/v2fly/v2ray-core/v5/common/net"
	"github.com/v2fly/v2ray-core/v5/common/session"
	"github.com/v2fly/v2ray-core/v5/features/outbound"
	"github.com/v2fly/v2ray-core/v5/features/policy"
	"github.com/v2fly/v2ray-core/v5/features/routing"
	"github.com/v2fly/v2ray-core/v5/transport"
)

type accessTestHandler struct {
	outbound.Handler
	tag        string
	dispatched bool
}

func (h *accessTestHandler) Tag() string                               { return h.tag }
func (h *accessTestHandler) Dispatch(context.Context, *transport.Link) { h.dispatched = true }

type accessTestManager struct {
	outbound.Manager
	handler outbound.Handler
}

func (m accessTestManager) GetHandler(string) outbound.Handler  { return m.handler }
func (m accessTestManager) GetDefaultHandler() outbound.Handler { return m.handler }

type accessTestPolicy struct{ policy.Manager }

func (accessTestPolicy) ForSystem() policy.System { return policy.System{} }

type accessTestRoute struct{ routing.Context }

func (accessTestRoute) GetOutboundTag() string         { return "direct" }
func (accessTestRoute) GetOutboundGroupTags() []string { return nil }

type accessTestRouter struct{ routing.Router }

func (accessTestRouter) PickRoute(routing.Context) (routing.Route, error) {
	return accessTestRoute{}, nil
}

type accessTestLogHandler struct{ messages []*log.AccessMessage }

func (h *accessTestLogHandler) Handle(msg log.Message) {
	if access, ok := msg.(*log.AccessMessage); ok {
		h.messages = append(h.messages, access)
	}
}

func TestRoutedDispatchAccessInboundTag(t *testing.T) {
	logs := &accessTestLogHandler{}
	log.RegisterHandler(logs)
	t.Cleanup(func() { log.RegisterHandler(&accessTestLogHandler{}) })
	for _, tc := range []struct {
		name, inbound, outbound, want string
		routed, missingInbound        bool
	}{
		{name: "matched route", inbound: "http-in", outbound: "direct", want: "http-in -> direct", routed: true},
		{name: "default outbound", inbound: "http-in", outbound: "direct", want: "http-in -> direct"},
		{name: "empty inbound", outbound: "direct", want: "direct"},
		{name: "missing inbound", outbound: "direct", want: "direct", missingInbound: true},
		{name: "empty outbound", inbound: "http-in"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := &accessTestHandler{tag: tc.outbound}
			d := &DefaultDispatcher{ohm: accessTestManager{handler: handler}, policy: accessTestPolicy{}}
			if tc.routed {
				d.router = accessTestRouter{}
			}
			ctx := context.Background()
			if !tc.missingInbound {
				ctx = session.ContextWithInbound(ctx, &session.Inbound{Tag: tc.inbound})
			}
			access := &log.AccessMessage{From: "127.0.0.1:39484", To: "//www.baidu.com:443", Status: log.AccessAccepted}
			ctx = log.ContextWithAccessMessage(ctx, access)
			before := len(logs.messages)
			d.routedDispatch(ctx, &transport.Link{}, net.TCPDestination(net.DomainAddress("www.baidu.com"), 443))
			if len(logs.messages) != before+1 || logs.messages[before] != access {
				t.Fatal("access message was not recorded exactly once")
			}
			if access.Detour != tc.want {
				t.Errorf("Detour = %q, want %q", access.Detour, tc.want)
			}
			want := "127.0.0.1:39484 accepted //www.baidu.com:443"
			if tc.want != "" {
				want += " [" + tc.want + "]"
			}
			if access.String() != want {
				t.Errorf("access log = %q, want %q", access.String(), want)
			}
			if !handler.dispatched {
				t.Fatal("outbound handler was not dispatched")
			}
		})
	}
}
