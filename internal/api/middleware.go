package api

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// KeyPrincipalEntry pairs an API key with its associated principal.
// RawKey holds the original key bytes; HashedKey is set at middleware init time.
type KeyPrincipalEntry struct {
	RawKey    []byte
	HashedKey [32]byte
	Principal AuthPrincipal
}

// LoggingMiddleware logs HTTP requests
func LoggingMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap response writer to capture status code
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			if ce := logger.Check(zap.DebugLevel, "http request"); ce != nil {
				ce.Write(
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
					zap.Int("status", wrapped.statusCode),
					zap.Duration("duration", time.Since(start)),
					zap.String("remote_addr", r.RemoteAddr),
				)
			}
		})
	}
}

// CORSConfig controls CORS behavior.
type CORSConfig struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
	MaxAgeSeconds  int
}

// DefaultCORSConfig returns permissive defaults suitable for local development.
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodDelete,
			http.MethodOptions,
		},
		AllowedHeaders: []string{"Content-Type", "Authorization", "X-API-Key"},
		MaxAgeSeconds:  86400,
	}
}

// CORSMiddleware adds CORS headers with default settings.
func CORSMiddleware(next http.Handler) http.Handler {
	return CORSMiddlewareWithConfig(DefaultCORSConfig())(next)
}

// CORSMiddlewareWithConfig adds CORS headers based on explicit configuration.
func CORSMiddlewareWithConfig(cfg CORSConfig) func(http.Handler) http.Handler {
	allowedOrigins := map[string]struct{}{}
	allowAnyOrigin := false
	for _, origin := range cfg.AllowedOrigins {
		trimmed := strings.TrimSpace(origin)
		if trimmed == "" {
			continue
		}
		if trimmed == "*" {
			allowAnyOrigin = true
			continue
		}
		allowedOrigins[trimmed] = struct{}{}
	}
	allowedMethods := normalizeCORSValues(cfg.AllowedMethods)
	if len(allowedMethods) == 0 {
		allowedMethods = []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions}
	}
	allowedHeaders := normalizeCORSValues(cfg.AllowedHeaders)
	if len(allowedHeaders) == 0 {
		allowedHeaders = []string{"Content-Type", "Authorization", "X-API-Key"}
	}
	maxAge := cfg.MaxAgeSeconds
	if maxAge < 0 {
		maxAge = 0
	}

	setCORSHeaders := func(w http.ResponseWriter, origin string) {
		if allowAnyOrigin {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(allowedMethods, ", "))
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(allowedHeaders, ", "))
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(maxAge))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := strings.TrimSpace(r.Header.Get("Origin"))
			if origin != "" {
				if !allowAnyOrigin {
					if _, ok := allowedOrigins[origin]; !ok {
						http.Error(w, "CORS origin not allowed", http.StatusForbidden)
						return
					}
				}
				setCORSHeaders(w, origin)
			}

			// Handle preflight requests
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// AuthMode controls API key behavior.
type AuthMode string

const (
	AuthModeOptional AuthMode = "optional"
	AuthModeRequired AuthMode = "required"
)

// AuthConfig configures auth middleware behavior.
type AuthConfig struct {
	APIKey            string
	Mode              AuthMode
	ExemptPaths       map[string]struct{}
	KeyPrincipals     []KeyPrincipalEntry
	LegacyNamespaceID string
}

// AdminRouteConfig controls path-based admin authorization.
type AdminRouteConfig struct {
	ExactPaths map[string]struct{}
	Prefixes   []string
}

// DefaultAuthExemptPaths returns read-only operational endpoints that are usually unauthenticated.
func DefaultAuthExemptPaths() map[string]struct{} {
	return map[string]struct{}{
		"/api/v1/health": {},
		"/health":        {},
	}
}

// DefaultAdminRouteConfig returns the supported admin-only paths.
func DefaultAdminRouteConfig() AdminRouteConfig {
	return AdminRouteConfig{
		ExactPaths: map[string]struct{}{
			"/api/v1/stats": {},
			"/stats":        {},
			"/metrics":      {},
		},
		Prefixes: []string{
			"/api/v1/admin/",
		},
	}
}

// APIKeyMiddleware validates API keys using legacy optional behavior.
func APIKeyMiddleware(apiKey string) func(http.Handler) http.Handler {
	return AuthMiddleware(AuthConfig{
		APIKey:      apiKey,
		Mode:        AuthModeOptional,
		ExemptPaths: DefaultAuthExemptPaths(),
	})
}

// constantTimeLookup iterates all key entries using HMAC + constant-time compare,
// avoiding timing oracles. The nonce ensures comparisons operate on fixed-size
// 32-byte HMAC digests regardless of original key lengths.
func constantTimeLookup(entries []KeyPrincipalEntry, nonce []byte, providedKey []byte) (AuthPrincipal, bool) {
	mac := hmac.New(sha256.New, nonce)
	mac.Write(providedKey)
	providedHash := mac.Sum(nil)

	matchIdx := -1
	for i := range entries {
		if subtle.ConstantTimeCompare(providedHash, entries[i].HashedKey[:]) == 1 {
			matchIdx = i
		}
	}
	if matchIdx < 0 {
		return AuthPrincipal{}, false
	}
	return entries[matchIdx].Principal, true
}

// hashKeyWithNonce computes HMAC-SHA256 of key using the given nonce.
func hashKeyWithNonce(nonce []byte, key []byte) [32]byte {
	mac := hmac.New(sha256.New, nonce)
	mac.Write(key)
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

// AuthMiddleware validates API keys according to configured mode.
func AuthMiddleware(cfg AuthConfig) func(http.Handler) http.Handler {
	exemptPaths := cfg.ExemptPaths
	if len(exemptPaths) == 0 {
		exemptPaths = DefaultAuthExemptPaths()
	}
	mode := cfg.Mode
	if mode == "" {
		mode = AuthModeOptional
	}

	// Generate a per-boot random nonce for HMAC key hashing. This ensures all
	// comparisons operate on 32-byte digests, eliminating length-leak side channels.
	var nonce [32]byte
	if len(cfg.KeyPrincipals) > 0 {
		if _, err := rand.Read(nonce[:]); err != nil {
			panic("failed to generate auth nonce: " + err.Error())
		}
		// Pre-hash all stored keys with the nonce at startup.
		for i := range cfg.KeyPrincipals {
			cfg.KeyPrincipals[i].HashedKey = hashKeyWithNonce(nonce[:], cfg.KeyPrincipals[i].RawKey)
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := exemptPaths[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}

			// Optional mode with no configured keys keeps local-dev ergonomics.
			if mode == AuthModeOptional && cfg.APIKey == "" && len(cfg.KeyPrincipals) == 0 {
				next.ServeHTTP(w, r)
				return
			}

			if cfg.APIKey == "" && len(cfg.KeyPrincipals) == 0 {
				http.Error(w, "Authentication misconfigured", http.StatusServiceUnavailable)
				return
			}

			// Check API key
			providedKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if providedKey == "" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			if len(cfg.KeyPrincipals) > 0 {
				principal, ok := constantTimeLookup(cfg.KeyPrincipals, nonce[:], []byte(providedKey))
				if !ok {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r.WithContext(withAuthPrincipal(r.Context(), principal)))
				return
			}

			if subtle.ConstantTimeCompare([]byte(providedKey), []byte(cfg.APIKey)) != 1 {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			if strings.TrimSpace(cfg.LegacyNamespaceID) != "" {
				principal := AuthPrincipal{NamespaceID: strings.TrimSpace(cfg.LegacyNamespaceID)}
				next.ServeHTTP(w, r.WithContext(withAuthPrincipal(r.Context(), principal)))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// AdminOnlyMiddleware restricts sensitive operational paths to admin principals.
func AdminOnlyMiddleware(cfg AdminRouteConfig) func(http.Handler) http.Handler {
	exactPaths := cfg.ExactPaths
	if len(exactPaths) == 0 && len(cfg.Prefixes) == 0 {
		defaults := DefaultAdminRouteConfig()
		exactPaths = defaults.ExactPaths
		cfg.Prefixes = defaults.Prefixes
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			protected := false
			if _, ok := exactPaths[path]; ok {
				protected = true
			} else {
				for _, prefix := range cfg.Prefixes {
					if strings.HasPrefix(path, prefix) {
						protected = true
						break
					}
				}
			}
			if !protected {
				next.ServeHTTP(w, r)
				return
			}

			principal, ok := AuthPrincipalFromContext(r.Context())
			if !ok {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			if !principal.Admin {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func normalizeCORSValues(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// ApplyMiddleware applies multiple middleware to a handler
func ApplyMiddleware(handler http.Handler, middleware ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}
