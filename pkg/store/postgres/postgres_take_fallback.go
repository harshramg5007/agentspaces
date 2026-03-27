package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func shouldFallbackToLegacyTake(err error) bool {
	sqlState := takeClaimSQLState(err)
	if sqlState == "" {
		return false
	}
	return strings.HasPrefix(sqlState, "42") || sqlState == "0A000"
}

func takeClaimSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
