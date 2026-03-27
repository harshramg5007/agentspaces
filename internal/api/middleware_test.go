package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCORSMiddlewareWithConfig_AllowsConfiguredOrigin(t *testing.T) {
	handler := CORSMiddlewareWithConfig(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{http.MethodGet, http.MethodPost, http.MethodOptions},
		AllowedHeaders: []string{"Content-Type", "X-API-Key"},
		MaxAgeSeconds:  120,
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("expected allow-origin header, got %q", got)
	}
}

func TestCORSMiddlewareWithConfig_RejectsDisallowedOrigin(t *testing.T) {
	handler := CORSMiddlewareWithConfig(CORSConfig{
		AllowedOrigins: []string{"https://allowed.example.com"},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	req.Header.Set("Origin", "https://blocked.example.com")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
}

func TestAuthMiddleware_RequiredMode(t *testing.T) {
	handler := AuthMiddleware(AuthConfig{
		APIKey: "secret",
		Mode:   AuthModeRequired,
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)
	req2.Header.Set("X-API-Key", "secret")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid key, got %d", rr2.Code)
	}
}

func TestAuthMiddleware_OptionalModeWithoutAPIKey(t *testing.T) {
	handler := AuthMiddleware(AuthConfig{
		Mode: AuthModeOptional,
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAdminOnlyMiddleware_ProtectsOperationalRoutes(t *testing.T) {
	handler := ApplyMiddleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		AuthMiddleware(AuthConfig{
			Mode: AuthModeRequired,
			KeyPrincipals: []KeyPrincipalEntry{
				{
					RawKey: []byte("user-key"),
					Principal: AuthPrincipal{
						NamespaceID: "tenant-a",
					},
				},
				{
					RawKey: []byte("admin-key"),
					Principal: AuthPrincipal{
						NamespaceID: "tenant-admin",
						Admin:       true,
					},
				},
			},
		}),
		AdminOnlyMiddleware(DefaultAdminRouteConfig()),
	)

	paths := []string{
		"/api/v1/admin/shards",
		"/api/v1/stats",
		"/stats",
		"/metrics",
	}
	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-API-Key", "user-key")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected 403 for non-admin path %s, got %d", path, rr.Code)
		}

		req = httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-API-Key", "admin-key")
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 for admin path %s, got %d", path, rr.Code)
		}
	}
}

func TestAdminOnlyMiddleware_LeavesHealthPublic(t *testing.T) {
	handler := ApplyMiddleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		AuthMiddleware(AuthConfig{
			Mode:        AuthModeOptional,
			ExemptPaths: DefaultAuthExemptPaths(),
		}),
		AdminOnlyMiddleware(DefaultAdminRouteConfig()),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for health without auth, got %d", rr.Code)
	}
}

func TestRateLimitMiddleware_MutatingOnly(t *testing.T) {
	mw := RateLimitMiddleware(RateLimitConfig{
		RequestsPerWindow: 1,
		Window:            time.Minute,
		MutatingOnly:      true,
	}, nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	getReq1 := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	getReq2 := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	postReq1 := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)
	postReq2 := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)

	getResp1 := httptest.NewRecorder()
	handler.ServeHTTP(getResp1, getReq1)
	getResp2 := httptest.NewRecorder()
	handler.ServeHTTP(getResp2, getReq2)
	if getResp1.Code != http.StatusOK || getResp2.Code != http.StatusOK {
		t.Fatalf("expected GET requests to bypass mutating rate limits")
	}

	postResp1 := httptest.NewRecorder()
	handler.ServeHTTP(postResp1, postReq1)
	postResp2 := httptest.NewRecorder()
	handler.ServeHTTP(postResp2, postReq2)
	if postResp1.Code != http.StatusOK {
		t.Fatalf("expected first POST to pass, got %d", postResp1.Code)
	}
	if postResp2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second POST to be rate limited, got %d", postResp2.Code)
	}
}

func TestMaxBodyBytesMiddleware(t *testing.T) {
	mw := MaxBodyBytesMiddleware(8, true)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if err != nil {
			if strings.Contains(err.Error(), "request body too large") {
				http.Error(w, "too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	okReq := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewBufferString("12345678"))
	okResp := httptest.NewRecorder()
	handler.ServeHTTP(okResp, okReq)
	if okResp.Code != http.StatusOK {
		t.Fatalf("expected 200 for body within limit, got %d", okResp.Code)
	}

	largeReq := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewBufferString("123456789"))
	largeResp := httptest.NewRecorder()
	handler.ServeHTTP(largeResp, largeReq)
	if largeResp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", largeResp.Code)
	}
}

func TestTelemetryModeMiddleware_Disabled(t *testing.T) {
	mw := TelemetryModeMiddleware("disabled")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	telemetryReq := httptest.NewRequest(http.MethodGet, "/api/v1/telemetry/spans?trace_id=x", nil)
	telemetryResp := httptest.NewRecorder()
	handler.ServeHTTP(telemetryResp, telemetryReq)
	if telemetryResp.Code != http.StatusNotFound {
		t.Fatalf("expected telemetry endpoint to be disabled, got %d", telemetryResp.Code)
	}

	healthReq := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	healthResp := httptest.NewRecorder()
	handler.ServeHTTP(healthResp, healthReq)
	if healthResp.Code != http.StatusOK {
		t.Fatalf("expected non-telemetry endpoint to pass, got %d", healthResp.Code)
	}
}
