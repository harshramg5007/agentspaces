package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestShouldFallbackToLegacyTake(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "sqlstate 42xxx falls back",
			err:  fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "42P01"}),
			want: true,
		},
		{
			name: "sqlstate 0A000 falls back",
			err:  &pgconn.PgError{Code: "0A000"},
			want: true,
		},
		{
			name: "sqlstate 57P01 does not fall back",
			err:  &pgconn.PgError{Code: "57P01"},
			want: false,
		},
		{
			name: "timeout does not fall back",
			err:  context.DeadlineExceeded,
			want: false,
		},
		{
			name: "generic error does not fall back",
			err:  errors.New("boom"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldFallbackToLegacyTake(tt.err)
			if got != tt.want {
				t.Fatalf("shouldFallbackToLegacyTake() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTakeClaimSQLState(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "extracts pg sqlstate",
			err:  fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "42P10"}),
			want: "42P10",
		},
		{
			name: "empty for non-pg error",
			err:  errors.New("plain"),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := takeClaimSQLState(tt.err)
			if got != tt.want {
				t.Fatalf("takeClaimSQLState() = %q, want %q", got, tt.want)
			}
		})
	}
}
