package chathttp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type Handler struct {
	Service chat.Service
	Secret  string
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
	parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || h.Secret == "" {
		respond(w, 401, "Invalid or expired token", nil)
		return chat.Actor{}, false
	}
	c := new(claims)
	token, err := jwt.ParseWithClaims(parts[1], c, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrTokenSignatureInvalid
		}
		return []byte(h.Secret), nil
	})
	if err != nil || !token.Valid || c.UserID == "" || (!c.IsParent && c.TenantID == "") {
		respond(w, 401, "Invalid or expired token", nil)
		return chat.Actor{}, false
	}
	user, e1 := parseID(c.UserID)
	tenant := uuid.Nil
	role := uuid.Nil
	member := uuid.Nil
	if c.TenantID != "" {
		tenant, e1 = uuid.Parse(c.TenantID)
		if e1 != nil {
			respond(w, 401, "Invalid or expired token", nil)
			return chat.Actor{}, false
		}
	}
	if c.RoleID != "" {
		role, _ = uuid.Parse(c.RoleID)
	}
	if c.MemberID != "" {
		member, _ = uuid.Parse(c.MemberID)
	}
	if e1 != nil || user == uuid.Nil || (!c.IsParent && tenant == uuid.Nil) {
		respond(w, 401, "Invalid or expired token", nil)
		return chat.Actor{}, false
	}
	return chat.Actor{UserID: user, TenantID: tenant, RoleID: role, MemberID: member, IsParent: c.IsParent}, true
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
				Kind string `json:"kind"`
			}
			if err := decode(r, &input); err != nil || input.Kind != "staff" {
				failure(w, chat.ErrInvalid)
				return
			}
			c, created, err := h.Service.Create(ctx, a)
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
