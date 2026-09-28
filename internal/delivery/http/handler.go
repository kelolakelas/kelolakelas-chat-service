package chathttp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type Handler struct {
	Service chat.Service
	Secret  string
	// Tickets mints single-use WS tickets; Hub fans out realtime events.
	// Both are wired by main. Conversation routes work when they are nil,
	// but the WS routes answer 500 until they are set.
	Tickets *chat.TicketStore
	Hub     *chat.WSHub
	// wsParams overrides connection liveness in tests. The zero value selects
	// the production defaults at serve time, so main never sets it.
	wsParams *wsParams
}
type claims struct {
	UserID   string `json:"user_id"`
	TenantID string `json:"tenant_id"`
	RoleID   string `json:"role_id"`
	MemberID string `json:"member_id"`
	IsParent bool   `json:"is_parent"`
	jwt.RegisteredClaims
}

func respond(w http.ResponseWriter, code int, message string, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": map[bool]string{true: "success", false: "error"}[code < 400], "message": message, "data": data})
}
func failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, chat.ErrNotFound):
		respond(w, 404, "Conversation not found", nil)
	case errors.Is(err, chat.ErrInvalid):
		respond(w, 400, "Invalid request", nil)
	case errors.Is(err, chat.ErrUnavailable):
		respond(w, 503, "Upstream service unavailable", nil)
	default:
		respond(w, 500, "Internal server error", nil)
	}
}
func parseID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, chat.ErrInvalid
	}
	return id, nil
}
func (h Handler) authenticate(w http.ResponseWriter, r *http.Request) (chat.Actor, bool) {
	a, _, ok := h.parseAuth(r)
	if !ok {
		respond(w, 401, "Invalid or expired token", nil)
		return chat.Actor{}, false
	}
	return a, true
}

// parseAuth validates the JWT and returns the actor plus the token expiry,
// so derived credentials (WS tickets) can be bound to the token lifetime.
func (h Handler) parseAuth(r *http.Request) (chat.Actor, time.Time, bool) {
	zero := chat.Actor{}
	parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || h.Secret == "" {
		return zero, time.Time{}, false
	}
	c := new(claims)
	token, err := jwt.ParseWithClaims(parts[1], c, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrTokenSignatureInvalid
		}
		return []byte(h.Secret), nil
	})
	if err != nil || !token.Valid || c.UserID == "" || (!c.IsParent && c.TenantID == "") {
		return zero, time.Time{}, false
	}
	user, e1 := parseID(c.UserID)
	tenant := uuid.Nil
	role := uuid.Nil
	member := uuid.Nil
	if c.TenantID != "" {
		tenant, e1 = uuid.Parse(c.TenantID)
		if e1 != nil {
			return zero, time.Time{}, false
		}
	}
	if c.RoleID != "" {
		role, _ = uuid.Parse(c.RoleID)
	}
	if c.MemberID != "" {
		member, _ = uuid.Parse(c.MemberID)
	}
	if e1 != nil || user == uuid.Nil || (!c.IsParent && tenant == uuid.Nil) {
		return zero, time.Time{}, false
	}
	var exp time.Time
	if raw, err := c.GetExpirationTime(); err == nil && raw != nil {
		exp = raw.Time
	}
	return chat.Actor{UserID: user, TenantID: tenant, RoleID: role, MemberID: member, IsParent: c.IsParent}, exp, true
}
func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	d := json.NewDecoder(io.LimitReader(r.Body, 8192))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return chat.ErrInvalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return chat.ErrInvalid
	}
	return nil
}
func positive(raw string, def, max int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > max {
		return 0, chat.ErrInvalid
	}
	return n, nil
}
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// WS realtime routes (KEL-121) dispatch before JWT: the ticket endpoint
	// authenticates inside issueWSTicket, while serveWS authenticates via
	// the single-use ticket itself.
	if r.URL.Path == "/api/v1/chat/ws-tickets" {
		if r.Method != http.MethodPost {
			respond(w, 405, "Method not allowed", nil)
			return
		}
		h.issueWSTicket(w, r)
		return
	}
	if r.URL.Path == "/api/v1/chat/ws" {
		if r.Method != http.MethodGet {
			respond(w, 405, "Method not allowed", nil)
			return
		}
		h.serveWS(w, r)
		return
	}
	a, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	base := "/api/v1/chat/conversations"
	path := strings.TrimPrefix(r.URL.Path, base)
	if path == "" || path == "/" {
		switch r.Method {
		case http.MethodGet:
			page, err := positive(r.URL.Query().Get("page"), 1, 1000000)
			if err != nil {
				failure(w, err)
				return
			}
			size, err := positive(r.URL.Query().Get("page_size"), 20, 100)
			if err != nil {
				failure(w, err)
				return
			}
			rows, err := h.Service.List(ctx, a, page, size)
			if err != nil {
				failure(w, err)
				return
			}
			respond(w, 200, "OK", rows)
		case http.MethodPost:
			var input struct {
				Kind      string `json:"kind"`
				SubjectID string `json:"subject_id"`
			}
			if err := decode(r, &input); err != nil || (input.Kind != "staff" && input.Kind != "schedule_request" && input.Kind != "report") {
				failure(w, chat.ErrInvalid)
				return
			}
			subject := uuid.Nil
			if input.Kind == "staff" {
				if input.SubjectID != "" {
					failure(w, chat.ErrInvalid)
					return
				}
			} else {
				var err error
				subject, err = parseID(input.SubjectID)
				if err != nil {
					failure(w, err)
					return
				}
			}
			c, created, err := h.Service.Create(ctx, a, chat.CreateInput{Kind: input.Kind, SubjectID: subject})
			if err != nil {
				failure(w, err)
				return
			}
			code := 200
			if created {
				code = 201
			}
			respond(w, code, "OK", c)
		default:
			respond(w, 405, "Method not allowed", nil)
		}
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	id, err := parseID(parts[0])
	if err != nil {
		failure(w, err)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		c, err := h.Service.Get(ctx, a, id)
		if err != nil {
			failure(w, err)
			return
		}
		respond(w, 200, "OK", c)
		return
	}
	if len(parts) == 2 && parts[1] == "messages" {
		switch r.Method {
		case http.MethodGet:
			limit, err := positive(r.URL.Query().Get("limit"), 20, 100)
			if err != nil {
				failure(w, err)
				return
			}
			var before *uuid.UUID
			if raw := r.URL.Query().Get("before"); raw != "" {
				v, err := parseID(raw)
				if err != nil {
					failure(w, err)
					return
				}
				before = &v
			}
			rows, err := h.Service.Messages(ctx, a, id, before, limit)
			if err != nil {
				failure(w, err)
				return
			}
			respond(w, 200, "OK", rows)
		case http.MethodPost:
			var input struct {
				Body            string `json:"body"`
				ClientMessageID string `json:"client_message_id"`
			}
			if err := decode(r, &input); err != nil {
				failure(w, err)
				return
			}
			m, err := h.Service.Send(ctx, a, id, input.Body, input.ClientMessageID)
			if err != nil {
				failure(w, err)
				return
			}
			respond(w, 201, "OK", m)
		default:
			respond(w, 405, "Method not allowed", nil)
		}
		return
	}
	if len(parts) == 2 && parts[1] == "read" && r.Method == http.MethodPost {
		if err := h.Service.Read(ctx, a, id); err != nil {
			failure(w, err)
			return
		}
		respond(w, 200, "OK", nil)
		return
	}
	respond(w, 404, "Not found", nil)
}
