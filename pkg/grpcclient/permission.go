package grpcclient

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	"time"
)

type Client struct {
	conn    *grpc.ClientConn
	timeout time.Duration
}

func New(target string, timeout time.Duration) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, timeout: timeout}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
func (c *Client) CheckPermission(ctx context.Context, tenant, role, member, permission string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := structpb.NewStruct(map[string]any{"tenant_id": tenant, "role_id": role, "member_id": member, "permission": permission})
	if err != nil {
		return false, err
	}
	resp := new(structpb.Struct)
	if err = c.conn.Invoke(ctx, "/tenant.PermissionService/CheckPermission", req, resp, grpc.StaticMethod()); err != nil {
		return false, err
	}
	value, ok := resp.GetFields()["allowed"]
	return ok && value.GetBoolValue(), nil
}
