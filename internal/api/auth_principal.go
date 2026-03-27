package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type authPrincipalContextKey struct{}

// NamespaceLimits configures per-namespace controls resolved from auth key records.
type NamespaceLimits struct {
	RateLimitRequests int           `json:"rate_limit_requests"`
	RateLimitWindow   time.Duration `json:"-"`
	MaxBodyBytes      int64         `json:"max_body_bytes"`
	MaxActiveAgents   int           `json:"max_active_agents"`
	MaxQueueDepth     int           `json:"max_queue_depth"`
}

// AuthPrincipal is attached to request context after successful authentication.
type AuthPrincipal struct {
	NamespaceID string
	Admin       bool
	Limits      NamespaceLimits
}

func withAuthPrincipal(ctx context.Context, principal AuthPrincipal) context.Context {
	return context.WithValue(ctx, authPrincipalContextKey{}, principal)
}

// AuthPrincipalFromContext returns the authenticated principal, if present.
func AuthPrincipalFromContext(ctx context.Context) (AuthPrincipal, bool) {
	raw := ctx.Value(authPrincipalContextKey{})
	if raw == nil {
		return AuthPrincipal{}, false
	}
	principal, ok := raw.(AuthPrincipal)
	if !ok {
		return AuthPrincipal{}, false
	}
	return principal, true
}

type authKeyFile struct {
	Keys []authKeyRecord `json:"keys"`
}

type authKeyRecord struct {
	APIKey      string            `json:"api_key"`
	NamespaceID string            `json:"namespace_id"`
	Admin       bool              `json:"admin"`
	Limits      authKeyLimitsFile `json:"limits"`
}

type authKeyLimitsFile struct {
	RateLimitRequests int    `json:"rate_limit_requests"`
	RateLimitWindow   string `json:"rate_limit_window"`
	MaxBodyBytes      int64  `json:"max_body_bytes"`
	MaxActiveAgents   int    `json:"max_active_agents"`
	MaxQueueDepth     int    `json:"max_queue_depth"`
}

// LoadAuthKeyPrincipals loads namespace-bound API key principals from a JSON file.
func LoadAuthKeyPrincipals(path string) ([]KeyPrincipalEntry, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read auth keys file: %w", err)
	}

	var cfg authKeyFile
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse auth keys file: %w", err)
	}

	out := make([]KeyPrincipalEntry, 0, len(cfg.Keys))
	seen := make(map[string]struct{}, len(cfg.Keys))
	for idx, rec := range cfg.Keys {
		apiKey := strings.TrimSpace(rec.APIKey)
		if apiKey == "" {
			return nil, fmt.Errorf("auth keys file entry %d missing api_key", idx)
		}
		namespaceID := strings.TrimSpace(rec.NamespaceID)
		if namespaceID == "" {
			return nil, fmt.Errorf("auth keys file entry %d missing namespace_id", idx)
		}
		if _, exists := seen[apiKey]; exists {
			return nil, fmt.Errorf("auth keys file contains duplicate api_key at entry %d", idx)
		}
		seen[apiKey] = struct{}{}

		limits := NamespaceLimits{
			RateLimitRequests: rec.Limits.RateLimitRequests,
			MaxBodyBytes:      rec.Limits.MaxBodyBytes,
			MaxActiveAgents:   rec.Limits.MaxActiveAgents,
			MaxQueueDepth:     rec.Limits.MaxQueueDepth,
		}
		if strings.TrimSpace(rec.Limits.RateLimitWindow) != "" {
			window, err := time.ParseDuration(strings.TrimSpace(rec.Limits.RateLimitWindow))
			if err != nil || window <= 0 {
				return nil, fmt.Errorf("auth keys file entry %d has invalid limits.rate_limit_window", idx)
			}
			limits.RateLimitWindow = window
		}
		if limits.RateLimitRequests < 0 || limits.MaxBodyBytes < 0 || limits.MaxActiveAgents < 0 || limits.MaxQueueDepth < 0 {
			return nil, fmt.Errorf("auth keys file entry %d has negative limit value", idx)
		}

		out = append(out, KeyPrincipalEntry{
			RawKey: []byte(apiKey),
			Principal: AuthPrincipal{
				NamespaceID: namespaceID,
				Admin:       rec.Admin,
				Limits:      limits,
			},
		})
	}

	return out, nil
}
