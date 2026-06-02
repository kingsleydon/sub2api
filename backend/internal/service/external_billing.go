package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// ErrInsufficientCredits is returned when the external billing service
// responds with 402, indicating the user does not have enough credits.
var ErrInsufficientCredits = errors.New("insufficient credits")

const externalBillingMaxAttempts = 3

var externalBillingRetryBackoffs = [...]time.Duration{
	100 * time.Millisecond,
	300 * time.Millisecond,
}

type externalBillingStatusError struct {
	statusCode int
}

func (e externalBillingStatusError) Error() string {
	return fmt.Sprintf("external billing: unexpected status %d", e.statusCode)
}

type externalBillingRequestError struct {
	err error
}

func (e externalBillingRequestError) Error() string {
	return fmt.Sprintf("external billing: request failed: %v", e.err)
}

func (e externalBillingRequestError) Unwrap() error {
	return e.err
}

// ExternalBillingService calls an external billing endpoint (Clawdi) to
// authorize requests. Usage is still written locally so ordered outbox sync can
// publish consumption to Clawdi and existing usage reports remain intact.
type ExternalBillingService struct {
	billingURL    string
	billingSecret string
	enabled       bool
	dryRun        bool
	client        *http.Client
}

// NewExternalBillingService creates a new ExternalBillingService from config.
func NewExternalBillingService(cfg *config.Config) *ExternalBillingService {
	return &ExternalBillingService{
		billingURL:    strings.TrimRight(cfg.Billing.ClawdiBillingURL, "/"),
		billingSecret: cfg.Billing.ClawdiBillingSecret,
		enabled:       cfg.Billing.IsExternalBillingEnabled(),
		dryRun:        cfg.Billing.IsExternalBillingDryRun(),
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 500 * time.Millisecond,
				}).DialContext,
				MaxIdleConns:        20,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// IsEnabled returns true when the external billing integration is active.
func (s *ExternalBillingService) IsEnabled() bool {
	return s.enabled
}

// IsDryRun returns true when the service is in dry-run mode (log only, don't enforce).
func (s *ExternalBillingService) IsDryRun() bool {
	return s.dryRun
}

type authorizeRequest struct {
	Sub2APIUserID int64  `json:"sub2api_user_id"`
	Model         string `json:"model"`
	Stream        bool   `json:"stream"`
}

// Authorize checks with the external billing service whether the user is
// allowed to make a request. Returns nil on success, ErrInsufficientCredits
// on 402, or a generic error on any other failure (fail-closed).
func (s *ExternalBillingService) Authorize(ctx context.Context, sub2apiUserID int64, model string, stream bool) error {
	// dry_run: always pass — no outbound call, avoids latency and malformed-URL errors
	if s.IsDryRun() {
		log.Printf("[ExternalBilling][DryRun] authorize pass for user %d model=%s", sub2apiUserID, model)
		return nil
	}

	var lastErr error
	for attempt := 0; attempt < externalBillingMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := s.authorizeOnce(ctx, sub2apiUserID, model, stream)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrInsufficientCredits) {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !isRetryableExternalBillingError(err) {
			return err
		}
		lastErr = err
		if attempt == externalBillingMaxAttempts-1 {
			break
		}
		if err := waitExternalBillingRetryBackoff(ctx, attempt); err != nil {
			return err
		}
	}
	return lastErr
}

func (s *ExternalBillingService) authorizeOnce(ctx context.Context, sub2apiUserID int64, model string, stream bool) error {
	reqBody := authorizeRequest{
		Sub2APIUserID: sub2apiUserID,
		Model:         model,
		Stream:        stream,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("external billing: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.billingURL+"/authorize", bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("external billing: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Billing-Secret", s.billingSecret)

	resp, err := s.client.Do(req)
	if err != nil {
		return externalBillingRequestError{err: err}
	}
	defer func() {
		// Drain response body to allow connection reuse
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusPaymentRequired:
		return ErrInsufficientCredits
	default:
		return externalBillingStatusError{statusCode: resp.StatusCode}
	}
}

func isRetryableExternalBillingError(err error) bool {
	var reqErr externalBillingRequestError
	if errors.As(err, &reqErr) {
		return true
	}

	var statusErr externalBillingStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	switch statusErr.statusCode {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func waitExternalBillingRetryBackoff(ctx context.Context, attempt int) error {
	delay := externalBillingRetryDelay(attempt)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func externalBillingRetryDelay(attempt int) time.Duration {
	if attempt < 0 || attempt >= len(externalBillingRetryBackoffs) {
		return 0
	}
	base := externalBillingRetryBackoffs[attempt]
	jitter := base / 10
	if jitter <= 0 {
		return base
	}
	delta := time.Duration(rand.Int64N(int64(jitter)*2+1)) - jitter
	delay := base + delta
	if delay < 0 {
		return 0
	}
	return delay
}
