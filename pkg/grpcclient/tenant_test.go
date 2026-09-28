package grpcclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type testTenantServer interface{ testTenant() }
type testTenantImpl struct{}

func (*testTenantImpl) testTenant() {}
func TestTenantNameWire(t *testing.T) {
	id := uuid.New()
	field := func(name string, n int32, kind descriptorpb.FieldDescriptorProto_Type, repeated bool, typeName string) *descriptorpb.FieldDescriptorProto {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(n), Type: kind.Enum(), Label: label.Enum(), TypeName: proto.String(typeName)}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: proto.String("test.proto"), Package: proto.String("tenant"), Syntax: proto.String("proto3"), MessageType: []*descriptorpb.DescriptorProto{
		{Name: proto.String("Request"), Field: []*descriptorpb.FieldDescriptorProto{field("tenant_ids", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, true, "")}},
		{Name: proto.String("Info"), Field: []*descriptorpb.FieldDescriptorProto{field("tenant_id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false, ""), field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false, "")}},
		{Name: proto.String("Response"), Field: []*descriptorpb.FieldDescriptorProto{field("tenants", 1, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, true, ".tenant.Info")}},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "tenant.TenantService", HandlerType: (*testTenantServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "GetTenantPublicInfo", Handler: func(_ any, _ context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		request := dynamicpb.NewMessage(file.Messages().Get(0))
		if err := decode(request); err != nil {
			return nil, err
		}
		if request.Get(file.Messages().Get(0).Fields().ByNumber(1)).List().Get(0).String() != id.String() {
			t.Error("wrong tenant")
		}
		info := dynamicpb.NewMessage(file.Messages().Get(1))
		info.Set(file.Messages().Get(1).Fields().ByNumber(1), protoreflect.ValueOfString(id.String()))
		info.Set(file.Messages().Get(1).Fields().ByNumber(2), protoreflect.ValueOfString("School"))
		response := dynamicpb.NewMessage(file.Messages().Get(2))
		response.Mutable(file.Messages().Get(2).Fields().ByNumber(1)).List().Append(protoreflect.ValueOfMessage(info))
		return response, nil
	}}}}, &testTenantImpl{})
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := &Client{conn: conn, timeout: time.Second}
	name, err := client.Name(context.Background(), id)
	if err != nil || name != "School" {
		t.Fatalf("name %q: %v", name, err)
	}
}
