package repository

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageLogBatchInsertQueries_WriteBillingOutbox(t *testing.T) {
	log := &service.UsageLog{
		UserID:       1,
		APIKeyID:     2,
		AccountID:    3,
		RequestID:    "req-outbox",
		Model:        "gpt-5",
		InputTokens:  10,
		OutputTokens: 5,
		TotalCost:    1.2,
		ActualCost:   1.2,
		CreatedAt:    time.Now().UTC(),
	}
	prepared := prepareUsageLogInsert(log)
	key := usageLogBatchKey(log.RequestID, log.APIKeyID)

	createQuery, _ := buildUsageLogBatchInsertQuery([]string{key}, map[string]usageLogInsertPrepared{key: prepared})
	require.Contains(t, createQuery, "INSERT INTO billing_usage_outbox")
	require.Contains(t, createQuery, "billing_usage_outbox_seq")
	require.Contains(t, createQuery, "RETURNING request_id, api_key_id, id, user_id, model, actual_cost, created_at")

	bestEffortQuery, _ := buildUsageLogBestEffortInsertQuery([]usageLogInsertPrepared{prepared})
	require.Contains(t, bestEffortQuery, "INSERT INTO billing_usage_outbox")
	require.Contains(t, bestEffortQuery, "billing_usage_outbox_seq")
	require.Contains(t, bestEffortQuery, "RETURNING id, user_id, model, actual_cost, created_at")
}
