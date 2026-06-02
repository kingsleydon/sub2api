package admin

import (
	"context"
	"crypto/subtle"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// BillingSyncHandler handles internal billing sync requests from the Clawdi backend.
type BillingSyncHandler struct {
	usageService billingSyncUsageService
	cfg          *config.Config
}

type billingSyncUsageService interface {
	GetUsageOutboxAfter(
		ctx context.Context,
		afterID int64,
		limit int,
		userID *int64,
	) ([]service.UsageOutboxItem, bool, error)
	AckUsageOutbox(ctx context.Context, ackedID int64) (int64, error)
}

// NewBillingSyncHandler creates a new billing sync handler.
func NewBillingSyncHandler(
	usageService billingSyncUsageService,
	cfg *config.Config,
) *BillingSyncHandler {
	return &BillingSyncHandler{
		usageService: usageService,
		cfg:          cfg,
	}
}

// UsageOutboxResponse is the JSON response for cursor-based usage sync.
type UsageOutboxResponse struct {
	AfterID     int64                 `json:"after_id"`
	NextAfterID int64                 `json:"next_after_id"`
	HasMore     bool                  `json:"has_more"`
	Items       []UsageOutboxItemJSON `json:"items"`
}

// UsageOutboxItemJSON is a single cursor-ordered usage event.
type UsageOutboxItemJSON struct {
	OutboxID       int64   `json:"outbox_id"`
	UsageLogID     int64   `json:"usage_log_id"`
	UserID         int64   `json:"user_id"`
	Model          string  `json:"model"`
	ActualCost     float64 `json:"actual_cost"`
	UsageCreatedAt string  `json:"usage_created_at"`
}

type UsageOutboxAckRequest struct {
	LastAcknowledgedOutboxID int64 `json:"last_acknowledged_outbox_id"`
}

type UsageOutboxAckResponse struct {
	LastAcknowledgedOutboxID int64 `json:"last_acknowledged_outbox_id"`
}

func (h *BillingSyncHandler) requireBillingSecret(c *gin.Context) bool {
	secret := c.GetHeader("X-Billing-Secret")
	expected := h.cfg.Billing.ClawdiBillingSecret
	if expected == "" || subtle.ConstantTimeCompare([]byte(secret), []byte(expected)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid billing secret"})
		return false
	}
	return true
}

// GetUsageOutbox handles cursor-based usage sync query.
//
//	GET /api/v1/internal/billing/usage-outbox
//	  ?after_id=0
//	  &limit=500
//	  &user_id=42  (optional)
//	Header: X-Billing-Secret: <secret>
func (h *BillingSyncHandler) GetUsageOutbox(c *gin.Context) {
	if !h.requireBillingSecret(c) {
		return
	}

	afterID := int64(0)
	if afterIDStr := c.DefaultQuery("after_id", "0"); afterIDStr != "" {
		parsed, err := strconv.ParseInt(afterIDStr, 10, 64)
		if err != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "after_id must be a non-negative integer"})
			return
		}
		afterID = parsed
	}

	limit := 500
	if limitStr := c.DefaultQuery("limit", "500"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed <= 0 || parsed > 2000 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 2000"})
			return
		}
		limit = parsed
	}

	var userID *int64
	if userIDStr := c.Query("user_id"); userIDStr != "" {
		id, err := strconv.ParseInt(userIDStr, 10, 64)
		if err != nil || id <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user_id"})
			return
		}
		userID = &id
	}

	items, hasMore, err := h.usageService.GetUsageOutboxAfter(
		c.Request.Context(),
		afterID,
		limit,
		userID,
	)
	if err != nil {
		log.Printf("[BillingOutbox] Failed to query after_id=%d limit=%d: %v", afterID, limit, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query usage outbox"})
		return
	}

	respItems := make([]UsageOutboxItemJSON, 0, len(items))
	nextAfterID := afterID
	for _, item := range items {
		respItems = append(respItems, UsageOutboxItemJSON{
			OutboxID:       item.OutboxID,
			UsageLogID:     item.UsageLogID,
			UserID:         item.UserID,
			Model:          item.Model,
			ActualCost:     item.ActualCost,
			UsageCreatedAt: item.UsageCreatedAt.UTC().Format(time.RFC3339),
		})
		nextAfterID = item.OutboxID
	}

	c.JSON(http.StatusOK, UsageOutboxResponse{
		AfterID:     afterID,
		NextAfterID: nextAfterID,
		HasMore:     hasMore,
		Items:       respItems,
	})
}

// AckUsageOutbox stores the latest cursor acknowledged by Clawdi.
//
//	POST /api/v1/internal/billing/usage-outbox/ack
//	Body: {"last_acknowledged_outbox_id": 12345}
//	Header: X-Billing-Secret: <secret>
func (h *BillingSyncHandler) AckUsageOutbox(c *gin.Context) {
	if !h.requireBillingSecret(c) {
		return
	}

	var req UsageOutboxAckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON body"})
		return
	}
	if req.LastAcknowledgedOutboxID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "last_acknowledged_outbox_id must be a positive integer"})
		return
	}

	ackID, err := h.usageService.AckUsageOutbox(
		c.Request.Context(),
		req.LastAcknowledgedOutboxID,
	)
	if err != nil {
		log.Printf("[BillingOutboxAck] Failed to ack id=%d: %v", req.LastAcknowledgedOutboxID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to ack usage outbox"})
		return
	}

	c.JSON(http.StatusOK, UsageOutboxAckResponse{
		LastAcknowledgedOutboxID: ackID,
	})
}
