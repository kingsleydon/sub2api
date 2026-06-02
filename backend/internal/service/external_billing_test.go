package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestExternalBillingAuthorizeRetriesTransientFailuresAndSucceeds(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/authorize", r.URL.Path)
		require.Equal(t, "secret", r.Header.Get("X-Billing-Secret"))
		if atomic.AddInt32(&attempts, 1) < externalBillingMaxAttempts {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	svc := newTestExternalBillingService(server.URL)
	err := svc.Authorize(context.Background(), 123, "gpt-5.5", true)

	require.NoError(t, err)
	require.Equal(t, int32(externalBillingMaxAttempts), atomic.LoadInt32(&attempts))
}

func TestExternalBillingAuthorizeStopsAfterMaxTransientFailures(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	svc := newTestExternalBillingService(server.URL)
	err := svc.Authorize(context.Background(), 123, "gpt-5.5", false)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected status 503")
	require.Equal(t, int32(externalBillingMaxAttempts), atomic.LoadInt32(&attempts))
}

func TestExternalBillingAuthorizeRetriesRequestTimeouts(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) < externalBillingMaxAttempts {
			time.Sleep(50 * time.Millisecond)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	svc := newTestExternalBillingService(server.URL)
	svc.client.Timeout = 10 * time.Millisecond
	err := svc.Authorize(context.Background(), 123, "gpt-5.5", false)

	require.NoError(t, err)
	require.Equal(t, int32(externalBillingMaxAttempts), atomic.LoadInt32(&attempts))
}

func TestExternalBillingAuthorizeDoesNotRetryInsufficientCredits(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusPaymentRequired)
	}))
	defer server.Close()

	svc := newTestExternalBillingService(server.URL)
	err := svc.Authorize(context.Background(), 123, "gpt-5.5", false)

	require.ErrorIs(t, err, ErrInsufficientCredits)
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts))
}

func TestExternalBillingAuthorizeDoesNotRetryDeterministicClientErrors(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	svc := newTestExternalBillingService(server.URL)
	err := svc.Authorize(context.Background(), 123, "gpt-5.5", false)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected status 401")
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts))
}

func TestExternalBillingAuthorizeStopsDuringBackoffWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		cancel()
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	svc := newTestExternalBillingService(server.URL)
	err := svc.Authorize(ctx, 123, "gpt-5.5", false)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts))
}

func TestExternalBillingRetryDelayUsesConfiguredBackoffsWithSmallJitter(t *testing.T) {
	for attempt, base := range externalBillingRetryBackoffs {
		for i := 0; i < 100; i++ {
			delay := externalBillingRetryDelay(attempt)
			require.GreaterOrEqual(t, delay, base-base/10)
			require.LessOrEqual(t, delay, base+base/10)
		}
	}
}

func TestNewExternalBillingServiceUsesProductionTimeouts(t *testing.T) {
	svc := NewExternalBillingService(&config.Config{
		Billing: config.BillingConfig{
			ClawdiBillingURL:    "https://api.example.com/internal/llm/",
			ClawdiBillingSecret: "secret",
			ClawdiBillingMode:   "production",
		},
	})

	require.True(t, svc.IsEnabled())
	require.Equal(t, "https://api.example.com/internal/llm", svc.billingURL)
	require.Equal(t, 2*time.Second, svc.client.Timeout)
}

func TestExternalBillingAuthorizeReturnsCanceledContextBeforeRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	svc := newTestExternalBillingService("http://127.0.0.1:1")
	err := svc.Authorize(ctx, 123, "gpt-5.5", false)

	require.True(t, errors.Is(err, context.Canceled))
}

func newTestExternalBillingService(url string) *ExternalBillingService {
	return &ExternalBillingService{
		billingURL:    url,
		billingSecret: "secret",
		enabled:       true,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}
