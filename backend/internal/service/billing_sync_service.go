package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	entsql "entgo.io/ent/dialect/sql"
)

// UsageOutboxItem is a single usage event entry from billing_usage_outbox.
type UsageOutboxItem struct {
	OutboxID       int64     `json:"outbox_id"`
	UsageLogID     int64     `json:"usage_log_id"`
	UserID         int64     `json:"user_id"`
	Model          string    `json:"model"`
	ActualCost     float64   `json:"actual_cost"`
	UsageCreatedAt time.Time `json:"usage_created_at"`
}

// rawDB extracts the underlying *sql.DB from the ent client driver.
func (s *UsageService) rawDB() (*sql.DB, error) {
	drv, ok := s.entClient.Driver().(*entsql.Driver)
	if !ok {
		return nil, fmt.Errorf("ent driver does not expose *sql.DB")
	}
	return drv.DB(), nil
}

// GetUsageOutboxAfter fetches a cursor-ordered batch from billing_usage_outbox.
//
// Returns at most "limit" items and a "hasMore" flag indicating whether
// additional rows remain after the returned batch.
func (s *UsageService) GetUsageOutboxAfter(
	ctx context.Context,
	afterID int64,
	limit int,
	userID *int64,
) ([]UsageOutboxItem, bool, error) {
	if limit <= 0 {
		limit = 500
	}
	limitPlusOne := limit + 1

	db, err := s.rawDB()
	if err != nil {
		return nil, false, err
	}

	// Fixed 3-param query: $2 is NULL when no user filter, avoiding fragile
	// positional parameter branching.
	query := `
		SELECT id, usage_log_id, user_id, model, actual_cost, usage_created_at
		FROM billing_usage_outbox
		WHERE id > $1
		  AND ($2::bigint IS NULL OR user_id = $2)
		ORDER BY id ASC
		LIMIT $3
	`

	rows, err := db.QueryContext(ctx, query, afterID, userID, limitPlusOne)
	if err != nil {
		return nil, false, fmt.Errorf("query usage outbox: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	items := make([]UsageOutboxItem, 0, limitPlusOne)
	for rows.Next() {
		var item UsageOutboxItem
		if err := rows.Scan(
			&item.OutboxID,
			&item.UsageLogID,
			&item.UserID,
			&item.Model,
			&item.ActualCost,
			&item.UsageCreatedAt,
		); err != nil {
			return nil, false, fmt.Errorf("scan usage outbox row: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate usage outbox rows: %w", err)
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

// AckUsageOutbox updates GC ack cursor monotonically and triggers
// asynchronous garbage collection of old outbox rows that have been processed.
func (s *UsageService) AckUsageOutbox(
	ctx context.Context,
	ackedID int64,
) (int64, error) {
	if ackedID <= 0 {
		return 0, fmt.Errorf("acked id must be positive")
	}

	db, err := s.rawDB()
	if err != nil {
		return 0, err
	}

	query := `
		INSERT INTO billing_usage_outbox_gc_state (id, last_acknowledged_outbox_id, updated_at)
		VALUES (1, $1, NOW())
		ON CONFLICT (id) DO UPDATE
		SET last_acknowledged_outbox_id = GREATEST(
			billing_usage_outbox_gc_state.last_acknowledged_outbox_id,
			EXCLUDED.last_acknowledged_outbox_id
		),
		    updated_at = NOW()
		RETURNING last_acknowledged_outbox_id
	`

	var updatedAckID int64
	if err := db.QueryRowContext(ctx, query, ackedID).Scan(&updatedAckID); err != nil {
		return 0, fmt.Errorf("ack usage outbox cursor: %w", err)
	}

	// Trigger GC asynchronously to avoid blocking the ACK HTTP response.
	go func() {
		gcCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.gcOutbox(gcCtx, updatedAckID); err != nil {
			log.Printf("[BillingOutboxGC] gc failed (ack_id=%d): %v", updatedAckID, err)
		}
	}()

	return updatedAckID, nil
}

// gcOutbox deletes outbox rows that are safely below the acknowledged cursor
// AND older than 1 year, per design doc §10.2.
// Deletes in batches with a max iteration cap to keep runtime bounded.
func (s *UsageService) gcOutbox(ctx context.Context, ackedID int64) error {
	const batchSize = 5000
	const maxBatches = 3

	if ackedID <= 0 {
		return nil
	}

	db, err := s.rawDB()
	if err != nil {
		return err
	}

	// Design doc §10.2: both conditions must be met simultaneously:
	//   id <= last_acknowledged_outbox_id
	//   AND created_at < now() - interval '1 year'
	query := `DELETE FROM billing_usage_outbox
		WHERE id IN (
			SELECT id FROM billing_usage_outbox
			WHERE id <= $1 AND created_at < NOW() - INTERVAL '1 year'
			ORDER BY id LIMIT $2
		)`

	var totalDeleted int64
	for i := 0; i < maxBatches; i++ {
		result, err := db.ExecContext(ctx, query, ackedID, batchSize)
		if err != nil {
			return fmt.Errorf("delete outbox rows: %w", err)
		}
		deleted, _ := result.RowsAffected()
		totalDeleted += deleted
		if deleted < int64(batchSize) {
			break
		}
	}

	if totalDeleted > 0 {
		log.Printf("[BillingOutboxGC] deleted %d rows (ack_id=%d)", totalDeleted, ackedID)
	}
	return nil
}
