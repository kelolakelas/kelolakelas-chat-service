package grpcclient

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type membershipWireServer interface{ membershipWire() }
type membershipWireImpl struct{}

func (*membershipWireImpl) membershipWire() {}
func TestMembershipWireDenialMalformedAndTimeout(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()
	var mode atomic.Int32
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "tenant.MembershipService", HandlerType: (*membershipWireServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "CheckActiveMembership", Handler: func(_ any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		req := new(structpb.Struct)
		if err := decode(req); err != nil {
			return nil, err
		}
		if req.Fields["tenant_id"].GetStringValue() != "tenant" || req.Fields["member_id"].GetStringValue() != "member" {
			t.Error("membership scope mismatch")
		}
		if mode.Load() == 3 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if mode.Load() == 2 {
			return structpb.NewStruct(map[string]any{"active": "wrong type"})
		}
		return structpb.NewStruct(map[string]any{"active": mode.Load() == 0})
	}}}}, &membershipWireImpl{})
	go server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///member", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &Client{conn: conn, timeout: time.Second}
	for _, m := range []int32{0, 1, 2, 3} {
		mode.Store(m)
		if m == 3 {
			c.timeout = 10 * time.Millisecond
		}
		allowed, err := c.CheckActiveMembership(context.Background(), "tenant", "member")
		if m < 2 {
			if err != nil || allowed != (m == 0) {
				t.Fatalf("mode=%d allowed=%v err=%v", m, allowed, err)
			}
		} else if err == nil || allowed {
			t.Fatalf("malformed/timeout became denial/success mode=%d", m)
		}
	}
}
