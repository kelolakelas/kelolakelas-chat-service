package chat

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func staffConv(tenant, member uuid.UUID) Conversation {
	return Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &member}
}

func tryRecv(c *WSConnection) ([]byte, bool) {
	select {
	case payload, ok := <-c.Out():
		return payload, ok
	default:
		return nil, false
	}
}

func mustRecv(t *testing.T, c *WSConnection) WSEvent {
	t.Helper()
	select {
	case payload, ok := <-c.Out():
		if !ok {
			t.Fatal("connection closed, wanted event")
		}
		var ev WSEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatalf("bad event payload: %v", err)
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return WSEvent{}
	}
}

func TestWSHubFanoutOnlyToVisible(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := staffConv(tenant, owner)
	manager := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true}
	denied := manager
	denied.UserID = uuid.New()
	denied.CanManage = false
	outsideTenant := uuid.New()
	outsider := Actor{UserID: uuid.New(), TenantID: outsideTenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true}
	otherParent := Actor{UserID: uuid.New(), IsParent: true}

	h := NewWSHub()
	cOwner, ok := h.Register(Actor{UserID: owner, TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()})
	if !ok {
		t.Fatal("owner register failed")
	}
	cManager, _ := h.Register(manager)
	cDenied, _ := h.Register(denied)
	cOutsider, _ := h.Register(outsider)
	cParent, _ := h.Register(otherParent)

	m := Message{ID: uuid.New(), ConversationID: conv.ID, SenderUserID: &manager.UserID, Body: "halo"}
	h.BroadcastMessage(conv, m)

	if ev := mustRecv(t, cOwner); ev.Type != WSEventMessageCreated || ev.Message == nil || ev.Message.Body != "halo" {
		t.Fatalf("owner event: %+v", ev)
	}
	if ev := mustRecv(t, cManager); ev.Type != WSEventMessageCreated || ev.ConversationID != conv.ID {
		t.Fatalf("manager event: %+v", ev)
	}
	for name, c := range map[string]*WSConnection{"denied member": cDenied, "other tenant": cOutsider, "other parent": cParent} {
		if payload, ok := tryRecv(c); ok {
			t.Fatalf("%s received leak: %s", name, payload)
		}
	}
}

func TestWSHubParentIsolation(t *testing.T) {
	tenant := uuid.New()
	parent := uuid.New()
	conv := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "schedule_request", ParentUserID: &parent}
	other := uuid.New()

	h := NewWSHub()
	cOwner, _ := h.Register(Actor{UserID: parent, IsParent: true})
	cOther, _ := h.Register(Actor{UserID: other, IsParent: true})
	cStaff, _ := h.Register(Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true})

	h.BroadcastRead(conv, parent, time.Now())

	if ev := mustRecv(t, cOwner); ev.Type != WSEventConversationRead || ev.UserID == nil || *ev.UserID != parent || ev.ReadAt == nil {
		t.Fatalf("owner read event: %+v", ev)
	}
	if ev := mustRecv(t, cStaff); ev.Type != WSEventConversationRead {
		t.Fatalf("staff read event: %+v", ev)
	}
	if payload, ok := tryRecv(cOther); ok {
		t.Fatalf("other parent received leak: %s", payload)
	}
}

func TestWSHubReportRequiresReportRight(t *testing.T) {
	tenant := uuid.New()
	parent := uuid.New()
	conv := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "report", ParentUserID: &parent}
	managerOnly := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true}
	reporter := managerOnly
	reporter.UserID = uuid.New()
	reporter.CanReport = true

	h := NewWSHub()
	cManager, _ := h.Register(managerOnly)
	cReporter, _ := h.Register(reporter)

	h.BroadcastMessage(conv, Message{ID: uuid.New(), ConversationID: conv.ID, Body: "r"})

	if ev := mustRecv(t, cReporter); ev.Type != WSEventMessageCreated {
		t.Fatalf("reporter event: %+v", ev)
	}
	if payload, ok := tryRecv(cManager); ok {
		t.Fatalf("chat:manage-only actor received report leak: %s", payload)
	}
}

// TestVisibleFrozenParityWithService pins the hub's frozen-rights gate to the
// live Service.Visible matrix. Any drift between the two is a leak-class bug:
// the mutation check breaks one gate and expects this test to fail.
func TestVisibleFrozenParityWithService(t *testing.T) {
	tenant := uuid.New()
	parent := uuid.New()
	member := uuid.New()
	staff := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &member}
	sched := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "schedule_request", ParentUserID: &parent}
	report := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "report", ParentUserID: &parent}
	notification := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "notification", SubjectID: parent, ParentUserID: &parent}

	actors := map[string]Actor{
		"staff owner":      {UserID: member, TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()},
		"permitted member": {UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true, CanReport: true},
		"unpermitted":      {UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()},
		"manage only":      {UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true},
		"other tenant":     {UserID: uuid.New(), TenantID: uuid.New(), RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true, CanReport: true},
		"owner parent":     {UserID: parent, IsParent: true},
		"other parent":     {UserID: uuid.New(), IsParent: true},
		"missing role":     {UserID: uuid.New(), TenantID: tenant, MemberID: uuid.New(), CanManage: true, CanReport: true},
		"anonymous":        {},
	}
	convs := map[string]Conversation{"staff": staff, "schedule_request": sched, "report": report, "notification": notification}

	for cname, conv := range convs {
		for aname, a := range actors {
			perms := permissionMatrix{}
			if a.CanManage {
				perms["chat:manage"] = true
			}
			if a.CanReport {
				perms["report:read"] = true
			}
			svc := Service{Permission: perms}
			// Freeze through the real connect-time path: Rights derives the
			// flags via allowed(), so a missing role/member can never keep
			// a true flag. Comparing against that (not a hand-set copy)
			// pins the production freeze end to end.
			frozen := svc.Rights(context.Background(), Actor{
				UserID: a.UserID, TenantID: a.TenantID, RoleID: a.RoleID,
				MemberID: a.MemberID, IsParent: a.IsParent,
			})
			want := svc.Visible(context.Background(), a, conv)
			if got := visibleFrozen(frozen, conv); got != want {
				t.Fatalf("drift %s/%s: frozen=%v live=%v", cname, aname, got, want)
			}
		}
	}
}

func TestWSHubConnLimitPerUser(t *testing.T) {
	h := NewWSHub()
	a := Actor{UserID: uuid.New(), TenantID: uuid.New()}
	var held []*WSConnection
	for i := 0; i < WSMaxConnsPerUser; i++ {
		c, ok := h.Register(a)
		if !ok {
			t.Fatalf("register %d refused", i)
		}
		held = append(held, c)
	}
	if _, ok := h.Register(a); ok {
		t.Fatal("6th connection accepted past the per-user cap")
	}
	// Another user is unaffected by the first user's cap.
	if _, ok := h.Register(Actor{UserID: uuid.New(), TenantID: uuid.New()}); !ok {
		t.Fatal("unrelated user refused")
	}
	// Releasing one slot lets the user reconnect.
	h.Unregister(held[0])
	if _, ok := h.Register(a); !ok {
		t.Fatal("register after unregister refused")
	}
}

func TestWSHubSlowConsumerEvicted(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := staffConv(tenant, owner)
	h := NewWSHub()
	slow, _ := h.Register(Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true})

	// Fill the buffer without any reader; the next broadcast must evict.
	for i := 0; i < WSConnSendBuffer; i++ {
		h.BroadcastMessage(conv, Message{ID: uuid.New(), ConversationID: conv.ID, Body: "x"})
	}
	if got := h.ConnCount(); got != 1 {
		t.Fatalf("conns=%d while buffer fills, want 1", got)
	}
	h.BroadcastMessage(conv, Message{ID: uuid.New(), ConversationID: conv.ID, Body: "overflow"})
	if got := h.ConnCount(); got != 0 {
		t.Fatalf("conns=%d after overflow, want 0 (slow evicted)", got)
	}
	// The buffered payloads stay readable; the channel is then closed.
	for i := 0; i < WSConnSendBuffer; i++ {
		if _, ok := <-slow.Out(); !ok {
			t.Fatalf("buffered payload %d lost on eviction", i)
		}
	}
	if _, ok := <-slow.Out(); ok {
		t.Fatal("slow channel still open after eviction")
	}
}

func TestWSHubHealthyConsumerKeepsUp(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := staffConv(tenant, owner)
	h := NewWSHub()
	healthy, _ := h.Register(Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true})
	// A consumer that drains every event synchronously never fills its
	// buffer, so bursts far past WSConnSendBuffer never evict it.
	const bursts = WSConnSendBuffer + 8
	for i := 0; i < bursts; i++ {
		h.BroadcastMessage(conv, Message{ID: uuid.New(), ConversationID: conv.ID, Body: "x"})
		select {
		case <-healthy.Out():
		case <-time.After(2 * time.Second):
			t.Fatalf("burst %d: draining consumer starved", i)
		}
	}
	if got := h.ConnCount(); got != 1 {
		t.Fatalf("conns=%d, want 1 (draining consumer kept)", got)
	}
	h.Close()
}

func TestWSHubCloseDropsAll(t *testing.T) {
	h := NewWSHub()
	c, _ := h.Register(Actor{UserID: uuid.New(), TenantID: uuid.New()})
	h.Close()
	if got := h.ConnCount(); got != 0 {
		t.Fatalf("conns=%d after close", got)
	}
	if _, ok := <-c.Out(); ok {
		t.Fatal("channel open after hub close")
	}
}
