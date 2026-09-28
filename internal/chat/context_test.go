package chat

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type contextReader struct {
	item SubjectContext
	err  error
}

func (r contextReader) Context(_ context.Context, _ string, _ uuid.UUID) (SubjectContext, error) {
	return r.item, r.err
}

type tenantReader struct{}

func (tenantReader) Name(context.Context, uuid.UUID) (string, error) { return "Tenant A", nil }

type permissionMatrix map[string]bool

func (p permissionMatrix) CheckPermission(_ context.Context, _, _, _, name string) (bool, error) {
	return p[name], nil
}

type captureStore struct {
	fakeStore
	received Conversation
}

func (f *captureStore) Create(_ context.Context, c Conversation) (Conversation, bool, error) {
	f.received = c
	c.ID = uuid.New()
	return c, true, nil
}
func TestContextCreateAndVisibility(t *testing.T) {
	tenant, parent, subject := uuid.New(), uuid.New(), uuid.New()
	source := SubjectContext{ID: subject, TenantID: tenant, ParentID: parent, ClassName: "Class", StudentFirstName: "Kid", Title: "Report"}
	owner := Actor{UserID: parent, IsParent: true}
	other := Actor{UserID: uuid.New(), IsParent: true}
	manager := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	outsider := manager
	outsider.TenantID = uuid.New()
	missingRole := manager
	missingRole.RoleID = uuid.Nil
	for _, kind := range []string{"schedule_request", "report"} {
		t.Run(kind, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				actor  Actor
				perms  permissionMatrix
				create bool
			}{
				{"parent owner", owner, nil, kind == "schedule_request"}, {"other parent", other, nil, false},
				{"authorized member", manager, permissionMatrix{"chat:manage": true, "report:read": true}, true},
				{"unpermitted member", manager, nil, false}, {"wrong tenant", outsider, permissionMatrix{"chat:manage": true, "report:read": true}, false},
				{"missing role", missingRole, permissionMatrix{"chat:manage": true, "report:read": true}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					store := &captureStore{}
					svc := Service{Store: store, Academic: contextReader{item: source}, Tenant: tenantReader{}, Permission: tc.perms}
					c, created, err := svc.Create(context.Background(), tc.actor, CreateInput{Kind: kind, SubjectID: subject})
					if tc.create {
						if err != nil || !created || c.ParentUserID == nil || *c.ParentUserID != parent || !svc.Visible(context.Background(), owner, c) {
							t.Fatalf("create %+v %v", c, err)
						}
					} else if !errors.Is(err, ErrNotFound) {
						t.Fatalf("denial: %v", err)
					}
					if tc.create && tc.actor.IsParent && svc.Visible(context.Background(), other, c) {
						t.Fatal("other parent visible")
					}
				})
			}
		})
	}
}
func TestContextVisibilityMatrix(t *testing.T) {
	tenant, parent := uuid.New(), uuid.New()
	for _, kind := range []string{"schedule_request", "report"} {
		c := Conversation{TenantID: tenant, Kind: kind, ParentUserID: &parent}
		permission := "chat:manage"
		if kind == "report" {
			permission = "report:read"
		}
		manager := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
		for _, tc := range []struct {
			name    string
			actor   Actor
			allowed bool
			want    bool
		}{
			{"owner", Actor{UserID: parent, IsParent: true}, false, true},
			{"other parent", Actor{UserID: uuid.New(), IsParent: true}, true, false},
			{"permitted member", manager, true, true},
			{"unpermitted member", manager, false, false},
			{"other tenant", Actor{UserID: manager.UserID, TenantID: uuid.New(), RoleID: manager.RoleID, MemberID: manager.MemberID}, true, false},
			{"missing membership", Actor{UserID: manager.UserID, TenantID: tenant, RoleID: manager.RoleID}, true, false},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				svc := Service{Permission: permissionMatrix{permission: tc.allowed}}
				if got := svc.Visible(context.Background(), tc.actor, c); got != tc.want {
					t.Fatalf("visible %v want %v", got, tc.want)
				}
			})
		}
	}
}
func TestContextErrors(t *testing.T) {
	tenant, parent, subject := uuid.New(), uuid.New(), uuid.New()
	actor := Actor{UserID: parent, IsParent: true}
	for _, err := range []error{ErrNotFound, ErrUnavailable} {
		svc := Service{Store: &captureStore{}, Academic: contextReader{err: err}, Tenant: tenantReader{}}
		if _, _, got := svc.Create(context.Background(), actor, CreateInput{Kind: "schedule_request", SubjectID: subject}); !errors.Is(got, err) {
			t.Fatal(got)
		}
	}
	svc := Service{Store: &captureStore{}, Academic: contextReader{item: SubjectContext{ID: subject, TenantID: tenant, ParentID: parent}}, Tenant: tenantReader{}}
	if _, _, err := svc.Create(context.Background(), actor, CreateInput{Kind: "report", SubjectID: subject}); err != ErrNotFound {
		t.Fatal(err)
	}
}
