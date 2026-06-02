package handler

import (
	"errors"
	"log"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// BillingCheckResult holds the HTTP error details when a billing check fails.
// If Err is nil the request is authorized.
type BillingCheckResult struct {
	Status     int
	Code       string
	Message    string
	RetryAfter int
	Err        error
}

// checkBillingEligibility dispatches to either the external Clawdi billing
// service or the local billing cache, returning a uniform result.
//
// Extracting this into a single function keeps the handler call-sites minimal
// and reduces merge-conflict surface when syncing upstream changes that touch
// the billing-check neighbourhood.
func checkBillingEligibility(
	c *gin.Context,
	externalBilling *service.ExternalBillingService,
	billingCache *service.BillingCacheService,
	user *service.User,
	apiKey *service.APIKey,
	group *service.Group,
	subscription *service.UserSubscription,
	model string,
	stream bool,
) *BillingCheckResult {
	if externalBilling != nil && externalBilling.IsEnabled() {
		if err := externalBilling.Authorize(c.Request.Context(), user.ID, model, stream); err != nil {
			if errors.Is(err, service.ErrInsufficientCredits) {
				return &BillingCheckResult{Status: http.StatusPaymentRequired, Code: "billing_error", Message: "Insufficient credits", Err: err}
			}
			log.Printf("External billing authorize failed: %v", err)
			return &BillingCheckResult{Status: http.StatusServiceUnavailable, Code: "billing_service_error", Message: "Billing service temporarily unavailable", Err: err}
		}
		return nil
	}
	if err := billingCache.CheckBillingEligibility(c.Request.Context(), user, apiKey, group, subscription); err != nil {
		status, code, message, retryAfter := billingErrorDetails(err)
		return &BillingCheckResult{Status: status, Code: code, Message: message, RetryAfter: retryAfter, Err: err}
	}
	return nil
}
