package chathttp

import (
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	"net/http"
)

func (h Handler) scheduleRequestPaymentLink(w http.ResponseWriter, r *http.Request) {
	if !internalCredentialOk(h.InternalCredential, r.Header.Get("X-Internal-Service-Credential")) {
		respond(w, http.StatusUnauthorized, "Invalid internal service credential", nil)
		return
	}
	var input chat.PaymentLinkInput
	if err := decode(r, &input); err != nil {
		failure(w, err)
		return
	}
	m, created, err := h.Service.SendScheduleRequestPaymentLink(r.Context(), input)
	if err != nil {
		failure(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	respond(w, code, "OK", m)
}
