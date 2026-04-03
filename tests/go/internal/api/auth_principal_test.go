package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAuthKeyPrincipals(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keys.json")
	content := `{
		"keys": [
			{
				"api_key": "key-a",
				"namespace_id": "team-a",
				"admin": true,
				"limits": {
					"rate_limit_requests": 50,
					"rate_limit_window": "2s",
					"max_body_bytes": 1024,
					"max_active_agents": 100,
					"max_queue_depth": 200
				}
			}
		]
	}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp auth file: %v", err)
	}

	principals, err := LoadAuthKeyPrincipals(path)
	if err != nil {
		t.Fatalf("LoadAuthKeyPrincipals() error = %v", err)
	}
	if len(principals) != 1 {
		t.Fatalf("expected 1 principal entry, got %d", len(principals))
	}
	entry := principals[0]
	if string(entry.RawKey) != "key-a" {
		t.Fatalf("expected raw key key-a, got %q", string(entry.RawKey))
	}
	principal := entry.Principal
	if principal.NamespaceID != "team-a" {
		t.Fatalf("expected namespace team-a, got %q", principal.NamespaceID)
	}
	if !principal.Admin {
		t.Fatalf("expected admin principal to be true")
	}
	if principal.Limits.RateLimitRequests != 50 {
		t.Fatalf("expected rate_limit_requests=50, got %d", principal.Limits.RateLimitRequests)
	}
	if principal.Limits.RateLimitWindow != 2*time.Second {
		t.Fatalf("expected rate_limit_window=2s, got %s", principal.Limits.RateLimitWindow)
	}
}

func TestAuthMiddleware_KeyPrincipalsInjectPrincipal(t *testing.T) {
	handler := AuthMiddleware(AuthConfig{
		Mode: AuthModeRequired,
		KeyPrincipals: []KeyPrincipalEntry{
			{
				RawKey: []byte("k1"),
				Principal: AuthPrincipal{
					NamespaceID: "ns-1",
					Limits: NamespaceLimits{
						RateLimitRequests: 3,
					},
				},
			},
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := AuthPrincipalFromContext(r.Context())
		if !ok {
			t.Fatalf("expected principal in request context")
		}
		if principal.NamespaceID != "ns-1" {
			t.Fatalf("expected namespace ns-1, got %q", principal.NamespaceID)
		}
		if principal.Limits.RateLimitRequests != 3 {
			t.Fatalf("expected limit 3, got %d", principal.Limits.RateLimitRequests)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)
	req.Header.Set("X-API-Key", "k1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}
