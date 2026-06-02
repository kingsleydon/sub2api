package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/util/mimeheader"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// effectiveTranscriptionModel is the model identifier used for billing and
// account selection. The ChatGPT OAuth upstream receives only the file part, so
// inbound model values are compatibility aliases for this shim.
const effectiveTranscriptionModel = "gpt-4o-transcribe"

var codexTranscriptionModelAliases = map[string]struct{}{
	effectiveTranscriptionModel:         {},
	"gpt-4o-mini-transcribe":            {},
	"gpt-4o-mini-transcribe-2025-03-20": {},
	"gpt-4o-mini-transcribe-2025-12-15": {},
	"whisper-1":                         {},
}

// AudioTranscriptions handles POST /v1/audio/transcriptions.
// Backed by ChatGPT/Codex's legacy OAuth transcription endpoint. The upstream
// accepts only a file part, so OpenAI-compatible fields that do not change the
// response shape are accepted at this boundary and dropped before forwarding.
func (h *OpenAIGatewayHandler) AudioTranscriptions(c *gin.Context) {
	streamStarted := false
	defer h.recoverResponsesPanic(c, &streamStarted)

	requestStart := time.Now()

	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.openai_gateway.audio_transcriptions",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)
	if !h.ensureResponsesDependencies(c, reqLog) {
		return
	}

	contentType := c.GetHeader("Content-Type")
	if contentType == "" {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Content-Type is required")
		return
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/form-data") {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Content-Type must be multipart/form-data")
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.errorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}
	if len(body) == 0 {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}
	if err := validateCodexTranscriptionMultipart(body, contentType); err != nil {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	requestedModel := extractMultipartTextField(body, contentType, "model")
	reqModel, ok := normalizeCodexTranscriptionModel(requestedModel)
	if !ok {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", unsupportedTranscriptionModelMessage(strings.TrimSpace(requestedModel)))
		return
	}
	if strings.TrimSpace(requestedModel) == "" {
		body, contentType, err = ensureMultipartTextField(body, contentType, "model", reqModel)
		if err != nil {
			h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse multipart form body")
			return
		}
	}
	reqStream := extractMultipartBoolField(body, contentType, "stream")
	setOpsRequestContext(c, reqModel, reqStream, body)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(reqStream, false)))

	if h.errorPassthroughService != nil {
		service.BindErrorPassthroughService(c, h.errorPassthroughService)
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)

	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())
	routingStart := time.Now()

	userReleaseFunc, acquired := h.acquireResponsesUserSlot(c, subject.UserID, subject.Concurrency, reqStream, &streamStarted, reqLog)
	if !acquired {
		return
	}
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	if br := checkBillingEligibility(c, h.externalBillingService, h.billingCacheService, apiKey.User, apiKey, apiKey.Group, subscription, reqModel, reqStream); br != nil {
		reqLog.Info("openai.audio_transcriptions.billing_eligibility_check_failed", zap.Error(br.Err))
		if br.RetryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(br.RetryAfter))
		}
		h.handleStreamingAwareError(c, br.Status, br.Code, br.Message, streamStarted)
		return
	}

	maxAccountSwitches := h.maxAccountSwitches
	switchCount := 0
	failedAccountIDs := make(map[int64]struct{})
	sameAccountRetryCount := make(map[int64]int)
	var lastFailoverErr *service.UpstreamFailoverError

	for {
		// First try with the model filter so model-mapped accounts are preferred;
		// fall back to any OpenAI OAuth account because Codex accounts don't
		// advertise transcription models, but still serve the transcription path.
		selection, err := h.gatewayService.SelectAccountWithLoadAwareness(c.Request.Context(), apiKey.GroupID, "", reqModel, failedAccountIDs)
		if err != nil && strings.TrimSpace(reqModel) != "" {
			selection, err = h.gatewayService.SelectAccountWithLoadAwareness(c.Request.Context(), apiKey.GroupID, "", "", failedAccountIDs)
		}
		if err != nil {
			if len(failedAccountIDs) == 0 {
				h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "api_error", "No available accounts: "+err.Error(), streamStarted)
				return
			}
			if lastFailoverErr != nil {
				h.handleFailoverExhausted(c, lastFailoverErr, streamStarted)
			} else {
				h.handleFailoverExhaustedSimple(c, http.StatusBadGateway, streamStarted)
			}
			return
		}
		if selection == nil || selection.Account == nil {
			h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "api_error", "No available accounts", streamStarted)
			return
		}

		account := selection.Account
		if !isOpenAIAudioTranscriptionAccount(account) {
			releaseIneligibleAccountSelection(selection)
			failedAccountIDs[account.ID] = struct{}{}
			continue
		}
		setOpsSelectedAccount(c, account.ID, account.Platform)

		accountReleaseFunc, acquired := h.acquireResponsesAccountSlot(c, apiKey.GroupID, "", selection, reqStream, &streamStarted, reqLog)
		if !acquired {
			return
		}

		service.SetOpsLatencyMs(c, service.OpsRoutingLatencyMsKey, time.Since(routingStart).Milliseconds())

		forwardStart := time.Now()
		result, fwdErr := h.gatewayService.ForwardAudioTranscription(c.Request.Context(), c, account, body, contentType)
		service.SetOpsLatencyMs(c, service.OpsResponseLatencyMsKey, time.Since(forwardStart).Milliseconds())
		if accountReleaseFunc != nil {
			accountReleaseFunc()
		}
		if fwdErr != nil {
			reqLog.Warn("openai.audio_transcriptions.forward_failed", zap.Int64("account_id", account.ID), zap.Error(fwdErr))
			var failoverErr *service.UpstreamFailoverError
			if errors.As(fwdErr, &failoverErr) {
				h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, false, nil)
				if failoverErr.RetryableOnSameAccount {
					retryLimit := account.GetPoolModeRetryCount()
					if sameAccountRetryCount[account.ID] < retryLimit {
						sameAccountRetryCount[account.ID]++
						reqLog.Warn("openai.audio_transcriptions.pool_mode_same_account_retry",
							zap.Int64("account_id", account.ID),
							zap.Int("upstream_status", failoverErr.StatusCode),
							zap.Int("retry_limit", retryLimit),
							zap.Int("retry_count", sameAccountRetryCount[account.ID]),
						)
						select {
						case <-c.Request.Context().Done():
							return
						case <-time.After(sameAccountRetryDelay):
						}
						continue
					}
				}
				h.gatewayService.RecordOpenAIAccountSwitch()
				failedAccountIDs[account.ID] = struct{}{}
				lastFailoverErr = failoverErr
				if switchCount >= maxAccountSwitches {
					h.handleFailoverExhausted(c, failoverErr, streamStarted)
					return
				}
				switchCount++
				reqLog.Warn("openai.audio_transcriptions.upstream_failover_switching",
					zap.Int64("account_id", account.ID),
					zap.Int("upstream_status", failoverErr.StatusCode),
					zap.Int("switch_count", switchCount),
					zap.Int("max_switches", maxAccountSwitches),
				)
				continue
			}
			h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, false, nil)
			return
		}

		h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, true, nil)

		userAgent := c.GetHeader("User-Agent")
		clientIP := ip.GetClientIP(c)
		go func(forward *service.OpenAIAudioTranscriptionForwardResult, usedAccount *service.Account, ua, ipAddr string, stream bool) {
			usage := service.OpenAIUsage{OutputTokens: forward.OutputTokens}
			recordInput := &service.OpenAIRecordUsageInput{
				Result: &service.OpenAIForwardResult{
					RequestID: forward.RequestID,
					Model:     effectiveTranscriptionModel,
					Usage:     usage,
					Stream:    stream,
					Duration:  forward.Duration,
				},
				APIKey:        apiKey,
				User:          apiKey.User,
				Account:       usedAccount,
				Subscription:  subscription,
				UserAgent:     ua,
				IPAddress:     ipAddr,
				APIKeyService: h.apiKeyService,
			}
			if err := h.gatewayService.RecordUsage(context.Background(), recordInput); err != nil {
				log.Printf("Record transcription usage failed: %v", err)
			}
		}(result, account, userAgent, clientIP, reqStream)
		return
	}
}

func isOpenAIAudioTranscriptionAccount(account *service.Account) bool {
	return account != nil && account.Type == service.AccountTypeOAuth && account.Platform == service.PlatformOpenAI
}

func releaseIneligibleAccountSelection(selection *service.AccountSelectionResult) {
	if selection != nil && selection.Acquired && selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func normalizeCodexTranscriptionModel(requested string) (string, bool) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return effectiveTranscriptionModel, true
	}
	_, ok := codexTranscriptionModelAliases[requested]
	if !ok {
		return "", false
	}
	return effectiveTranscriptionModel, true
}

func unsupportedTranscriptionModelMessage(model string) string {
	return "model " + strconv.Quote(model) + " is not supported for Codex OAuth transcription; use " + strconv.Quote(effectiveTranscriptionModel)
}

func validateCodexTranscriptionMultipart(body []byte, contentType string) error {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return err
	}
	boundary, ok := params["boundary"]
	if !ok || strings.TrimSpace(boundary) == "" {
		return errors.New("missing multipart boundary")
	}

	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	hasFile := false
	for {
		part, err := reader.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		formName := part.FormName()
		switch formName {
		case "file":
			hasFile = true
		case "model":
			value, err := readMultipartTextValue(part)
			if err != nil {
				_ = part.Close()
				return err
			}
			if _, ok := normalizeCodexTranscriptionModel(value); !ok {
				_ = part.Close()
				return errors.New(unsupportedTranscriptionModelMessage(value))
			}
		case "language", "prompt", "temperature":
			_, _ = io.Copy(io.Discard, part)
		case "response_format":
			value, err := readMultipartTextValue(part)
			if err != nil {
				_ = part.Close()
				return err
			}
			if value != "" && value != "json" {
				_ = part.Close()
				return errors.New("response_format " + strconv.Quote(value) + " is not supported for Codex OAuth transcription; use \"json\"")
			}
		case "stream":
			value, err := readMultipartTextValue(part)
			if err != nil {
				_ = part.Close()
				return err
			}
			switch strings.ToLower(value) {
			case "", "0", "false", "no", "off":
			default:
				_ = part.Close()
				return errors.New("stream is not supported by Codex OAuth transcription")
			}
		default:
			_, _ = io.Copy(io.Discard, part)
			_ = part.Close()
			if formName == "" {
				return errors.New("multipart field name is required")
			}
			return errors.New("parameter " + strconv.Quote(formName) + " is not supported by Codex OAuth transcription")
		}

		if err := part.Close(); err != nil {
			return err
		}
	}
	if !hasFile {
		return errors.New("multipart body missing file field")
	}
	return nil
}

func readMultipartTextValue(part *multipart.Part) (string, error) {
	value, err := io.ReadAll(io.LimitReader(part, 64<<10))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func ensureMultipartTextField(body []byte, contentType, fieldName, value string) ([]byte, string, error) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, "", err
	}
	boundary, ok := params["boundary"]
	if !ok || strings.TrimSpace(boundary) == "" {
		return nil, "", errors.New("missing multipart boundary")
	}

	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	found := false

	for {
		part, err := reader.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, "", err
		}

		if part.FormName() == fieldName {
			found = true
		}

		header := mimeheader.Clone(part.Header)
		partWriter, err := writer.CreatePart(header)
		if err != nil {
			_ = part.Close()
			return nil, "", err
		}
		if _, err := io.Copy(partWriter, part); err != nil {
			_ = part.Close()
			return nil, "", err
		}
		if err := part.Close(); err != nil {
			return nil, "", err
		}
	}

	if !found {
		if err := writer.WriteField(fieldName, value); err != nil {
			_ = writer.Close()
			return nil, "", err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), writer.FormDataContentType(), nil
}

func extractMultipartBoolField(body []byte, contentType, fieldName string) bool {
	v := strings.ToLower(strings.TrimSpace(extractMultipartTextField(body, contentType, fieldName)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func extractMultipartTextField(body []byte, contentType, fieldName string) string {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	boundary, ok := params["boundary"]
	if !ok || boundary == "" {
		return ""
	}

	r := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := r.NextPart()
		if err != nil {
			return ""
		}
		if part.FormName() != fieldName {
			_ = part.Close()
			continue
		}
		value, readErr := io.ReadAll(part)
		_ = part.Close()
		if readErr != nil {
			return ""
		}
		return string(value)
	}
}
