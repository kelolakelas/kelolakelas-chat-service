package chathttp

import (
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	"net/http"
)

func (h Handler) privateLifecycle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !internalCredentialOk(h.InternalCredential, r.Header.Get("X-Internal-Service-Credential")) {
		respond(w, 401, "Invalid internal service credential", nil)
		return
	}
	var input chat.PrivateLifecycleInput
	if err := decode(r, &input); err != nil {
		failure(w, err)
		return
	}
	m, created, err := h.Service.SendPrivateLifecycle(r.Context(), input)
	if err != nil {
		failure(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	respond(w, status, "OK", m)
}
