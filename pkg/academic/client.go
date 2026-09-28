package academic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type Client struct {
	BaseURL, Credential string
	HTTP                *http.Client
}

func New(baseURL, credential string, timeout time.Duration) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || credential == "" {
		return nil, errors.New("invalid academic configuration")
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Credential: credential, HTTP: &http.Client{Timeout: timeout}}, nil
}
func (c *Client) Context(ctx context.Context, kind string, id uuid.UUID) (chat.SubjectContext, error) {
	path := "/internal/chat-context/schedule-requests/"
	if kind == "report" {
		path = "/internal/chat-context/reports/"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path+id.String(), nil)
	if err != nil {
		return chat.SubjectContext{}, chat.ErrUnavailable
	}
	req.Header.Set("X-Internal-Service-Credential", c.Credential)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return chat.SubjectContext{}, chat.ErrUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return chat.SubjectContext{}, chat.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return chat.SubjectContext{}, chat.ErrUnavailable
	}
	var envelope struct {
		Data struct {
			ID               uuid.UUID `json:"id"`
			TenantID         uuid.UUID `json:"tenant_id"`
			ParentID         uuid.UUID `json:"parent_id"`
			ClassName        string    `json:"class_name"`
			StudentFirstName string    `json:"student_first_name"`
			Title            string    `json:"title"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16384)).Decode(&envelope); err != nil {
		return chat.SubjectContext{}, chat.ErrUnavailable
	}
	data := envelope.Data
	return chat.SubjectContext{ID: data.ID, TenantID: data.TenantID, ParentID: data.ParentID, ClassName: data.ClassName, StudentFirstName: data.StudentFirstName, Title: data.Title}, nil
}
