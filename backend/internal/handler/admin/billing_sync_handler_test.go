package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type stubBillingSyncUsageService struct {
	getUsageOutboxAfter func(ctx context.Context, afterID int64, limit int, userID *int64) ([]service.UsageOutboxItem, bool, error)
	ackUsageOutbox      func(ctx context.Context, ackedID int64) (int64, error)
}

func (s *stubBillingSyncUsageService) GetUsageOutboxAfter(
	ctx context.Context,
	afterID int64,
	limit int,
	userID *int64,
) ([]service.UsageOutboxItem, bool, error) {
	if s.getUsageOutboxAfter != nil {
		return s.getUsageOutboxAfter(ctx, afterID, limit, userID)
	}
	return []service.UsageOutboxItem{}, false, nil
}

func (s *stubBillingSyncUsageService) AckUsageOutbox(ctx context.Context, ackedID int64) (int64, error) {
	if s.ackUsageOutbox != nil {
		return s.ackUsageOutbox(ctx, ackedID)
	}
	return ackedID, nil
}

func newBillingSyncTestHandler(svc billingSyncUsageService) *BillingSyncHandler {
	if svc == nil {
		svc = &stubBillingSyncUsageService{}
	}
	return NewBillingSyncHandler(
		svc,
		&config.Config{
			Billing: config.BillingConfig{
				ClawdiBillingSecret: "test-secret",
			},
		},
	)
}

func TestGetUsageOutboxRejectsNegativeAfterID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := newBillingSyncTestHandler(nil)
	router.GET("/api/v1/internal/billing/usage-outbox", handler.GetUsageOutbox)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/billing/usage-outbox?after_id=-1", nil)
	req.Header.Set("X-Billing-Secret", "test-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "after_id must be a non-negative integer")
}

func TestGetUsageOutboxRejectsInvalidLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := newBillingSyncTestHandler(nil)
	router.GET("/api/v1/internal/billing/usage-outbox", handler.GetUsageOutbox)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/internal/billing/usage-outbox?after_id=0&limit=5001",
		nil,
	)
	req.Header.Set("X-Billing-Secret", "test-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "limit must be between 1 and 2000")
}

func TestGetUsageOutboxRejectsInvalidSecret(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := newBillingSyncTestHandler(nil)
	router.GET("/api/v1/internal/billing/usage-outbox", handler.GetUsageOutbox)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/internal/billing/usage-outbox?after_id=0&limit=10",
		nil,
	)
	req.Header.Set("X-Billing-Secret", "wrong-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Body.String(), "Invalid billing secret")
}

func TestGetUsageOutboxReturnsItemsAndCursor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	var capturedAfterID int64
	var capturedLimit int
	var capturedUserID *int64
	now := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)

	stub := &stubBillingSyncUsageService{
		getUsageOutboxAfter: func(
			_ context.Context,
			afterID int64,
			limit int,
			userID *int64,
		) ([]service.UsageOutboxItem, bool, error) {
			capturedAfterID = afterID
			capturedLimit = limit
			capturedUserID = userID
			return []service.UsageOutboxItem{
				{
					OutboxID:       1,
					UsageLogID:     1001,
					UserID:         42,
					Model:          "gpt-4o",
					ActualCost:     0.02,
					UsageCreatedAt: now,
				},
				{
					OutboxID:       2,
					UsageLogID:     1002,
					UserID:         42,
					Model:          "gpt-4o-mini",
					ActualCost:     0.01,
					UsageCreatedAt: now.Add(time.Minute),
				},
			}, true, nil
		},
	}

	handler := newBillingSyncTestHandler(stub)
	router.GET("/api/v1/internal/billing/usage-outbox", handler.GetUsageOutbox)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/internal/billing/usage-outbox?after_id=0&limit=2&user_id=42",
		nil,
	)
	req.Header.Set("X-Billing-Secret", "test-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"after_id":0`)
	require.Contains(t, rec.Body.String(), `"next_after_id":2`)
	require.Contains(t, rec.Body.String(), `"has_more":true`)
	require.Contains(t, rec.Body.String(), `"outbox_id":1`)
	require.Contains(t, rec.Body.String(), `"outbox_id":2`)

	require.EqualValues(t, 0, capturedAfterID)
	require.Equal(t, 2, capturedLimit)
	require.NotNil(t, capturedUserID)
	require.EqualValues(t, 42, *capturedUserID)
}

func TestGetUsageOutboxServiceErrorReturns500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	stub := &stubBillingSyncUsageService{
		getUsageOutboxAfter: func(_ context.Context, _ int64, _ int, _ *int64) ([]service.UsageOutboxItem, bool, error) {
			return nil, false, errors.New("db down")
		},
	}

	handler := newBillingSyncTestHandler(stub)
	router.GET("/api/v1/internal/billing/usage-outbox", handler.GetUsageOutbox)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/billing/usage-outbox", nil)
	req.Header.Set("X-Billing-Secret", "test-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "Failed to query usage outbox")
}

func TestAckUsageOutboxRejectsNegativeID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := newBillingSyncTestHandler(nil)
	router.POST("/api/v1/internal/billing/usage-outbox/ack", handler.AckUsageOutbox)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/internal/billing/usage-outbox/ack",
		strings.NewReader(`{"last_acknowledged_outbox_id":-1}`),
	)
	req.Header.Set("X-Billing-Secret", "test-secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "last_acknowledged_outbox_id must be a positive integer")
}

func TestAckUsageOutboxRejectsZeroID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := newBillingSyncTestHandler(nil)
	router.POST("/api/v1/internal/billing/usage-outbox/ack", handler.AckUsageOutbox)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/internal/billing/usage-outbox/ack",
		strings.NewReader(`{"last_acknowledged_outbox_id":0}`),
	)
	req.Header.Set("X-Billing-Secret", "test-secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "last_acknowledged_outbox_id must be a positive integer")
}

func TestAckUsageOutboxRejectsInvalidSecret(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := newBillingSyncTestHandler(nil)
	router.POST("/api/v1/internal/billing/usage-outbox/ack", handler.AckUsageOutbox)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/internal/billing/usage-outbox/ack",
		strings.NewReader(`{"last_acknowledged_outbox_id":1}`),
	)
	req.Header.Set("X-Billing-Secret", "wrong-secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Body.String(), "Invalid billing secret")
}

func TestAckUsageOutboxSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	var capturedAckID int64
	stub := &stubBillingSyncUsageService{
		ackUsageOutbox: func(_ context.Context, ackedID int64) (int64, error) {
			capturedAckID = ackedID
			return 7, nil
		},
	}

	handler := newBillingSyncTestHandler(stub)
	router.POST("/api/v1/internal/billing/usage-outbox/ack", handler.AckUsageOutbox)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/internal/billing/usage-outbox/ack",
		strings.NewReader(`{"last_acknowledged_outbox_id":5}`),
	)
	req.Header.Set("X-Billing-Secret", "test-secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"last_acknowledged_outbox_id":7`)
	require.EqualValues(t, 5, capturedAckID)
}
