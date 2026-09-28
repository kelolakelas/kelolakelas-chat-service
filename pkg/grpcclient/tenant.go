package grpcclient

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// The public-info RPC uses its small wire contract without importing identity's Go module.
func (c *Client) Name(ctx context.Context, tenant uuid.UUID) (string, error) {
	label := func(s string) *string { return &s }
	number := func(n int32) *int32 { return &n }
	field := func(name string, n int32, typ descriptorpb.FieldDescriptorProto_Type, repeated bool, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{Name: label(name), Number: number(n), Type: &typ, TypeName: label(typeName)}
		mode := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if repeated {
			mode = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		f.Label = &mode
		return f
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: label("chat_tenant_public_info.proto"), Package: label("tenant"), Syntax: label("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: label("TenantPublicInfoRequest"), Field: []*descriptorpb.FieldDescriptorProto{field("tenant_ids", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, true, "")}},
			{Name: label("TenantPublicInfo"), Field: []*descriptorpb.FieldDescriptorProto{field("tenant_id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false, ""), field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false, "")}},
			{Name: label("TenantPublicInfoResponse"), Field: []*descriptorpb.FieldDescriptorProto{field("tenants", 1, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, true, ".tenant.TenantPublicInfo")}},
		},
	}, nil)
	if err != nil {
		return "", err
	}
	messages := file.Messages()
	reqDesc, infoDesc, respDesc := messages.Get(0), messages.Get(1), messages.Get(2)
	req := dynamicpb.NewMessage(reqDesc)
	req.Mutable(reqDesc.Fields().ByNumber(1)).List().Append(protoreflect.ValueOfString(tenant.String()))
	resp := dynamicpb.NewMessage(respDesc)
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := c.conn.Invoke(callCtx, "/tenant.TenantService/GetTenantPublicInfo", req, resp); err != nil {
		return "", err
	}
	items := resp.Get(respDesc.Fields().ByNumber(1)).List()
	for i := 0; i < items.Len(); i++ {
		info := items.Get(i).Message()
		if info.Get(infoDesc.Fields().ByNumber(1)).String() == tenant.String() {
			return info.Get(infoDesc.Fields().ByNumber(2)).String(), nil
		}
	}
	return "", errors.New("tenant not found")
}
