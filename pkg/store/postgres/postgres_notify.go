package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

func (s *PostgresStore) startWakeupListener(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		return fmt.Errorf("connect postgres wakeup listener: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgQuoteIdentifier(postgresWakeupChannel)); err != nil {
		_ = conn.Close(context.Background())
		return fmt.Errorf("listen on %s: %w", postgresWakeupChannel, err)
	}
	s.listenerConn = conn
	go s.listenForWakeups()
	return nil
}

func (s *PostgresStore) listenForWakeups() {
	for {
		if s.ctx.Err() != nil {
			return
		}
		notification, err := s.listenerConn.WaitForNotification(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			s.logger.Warn("postgres wakeup listener error", zap.Error(err))
			s.signalWakeups()
			continue
		}
		if notification != nil {
			s.signalWakeups()
		}
	}
}

func (s *PostgresStore) currentWakeupChan() <-chan struct{} {
	s.wakeupMu.RLock()
	defer s.wakeupMu.RUnlock()
	return s.wakeupCh
}

func (s *PostgresStore) signalWakeups() {
	s.wakeupMu.Lock()
	defer s.wakeupMu.Unlock()
	close(s.wakeupCh)
	s.wakeupCh = make(chan struct{})
}

func queueWakeupNotify(ctx context.Context, tx pgx.Tx, namespaceID, queue string) error {
	_, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", postgresWakeupChannel, buildWakeupPayload(namespaceID, queue))
	return err
}

func buildWakeupPayload(namespaceID, queue string) string {
	namespaceID = strings.TrimSpace(namespaceID)
	queue = strings.TrimSpace(queue)
	return namespaceID + "|" + queue
}

func pgQuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func pgQuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
