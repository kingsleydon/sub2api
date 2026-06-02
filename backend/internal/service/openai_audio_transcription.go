package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/util/mimeheader"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"

	"github.com/gin-gonic/gin"
)

// transcriptionSSELineBufferSize bounds a single SSE event line to keep memory
// growth bounded if the upstream emits an unusually long transcript line.
const transcriptionSSELineBufferSize = 1 << 20 // 1 MiB

// chatGPTTranscriptionPath is the ChatGPT/Codex OAuth transcription endpoint
// used by Codex voice input. The public OpenAI-compatible inbound route remains
// /v1/audio/transcriptions.
const chatGPTTranscriptionPath = "/backend-api/transcribe"

// OpenAIAudioTranscriptionForwardResult is the lightweight result returned by
// ForwardAudioTranscription so the handler can record usage. The upstream
// transcription endpoint does not emit a usage block in either the SSE or the
// final JSON response, so OutputTokens is estimated from the produced text.
type OpenAIAudioTranscriptionForwardResult struct {
	RequestID    string
	Duration     time.Duration
	OutputTokens int
}

// ForwardAudioTranscription forwards a multipart /v1/audio/transcriptions
// request to ChatGPT/Codex's OAuth-backed transcription upstream. The endpoint
// is materially more stable when the request enforces TLS 1.3 and uses a
// Chrome-like client fingerprint, so we attach UpstreamRequestOptions accordingly.
func (s *OpenAIGatewayService) ForwardAudioTranscription(ctx context.Context, c *gin.Context, account *Account, body []byte, contentType string) (*OpenAIAudioTranscriptionForwardResult, error) {
	startTime := time.Now()

	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}

	reqBody := body
	reqContentType := contentType
	if account.Type == AccountTypeOAuth {
		convertedBody, convertedType, convErr := convertTranscriptionMultipartForOAuth(body, contentType)
		if convErr != nil {
			return nil, fmt.Errorf("invalid transcription multipart body: %w", convErr)
		}
		reqBody = convertedBody
		reqContentType = convertedType
	}

	reqOpts := UpstreamRequestOptions{
		MinTLSVersion: tls.VersionTLS13,
	}
	if account.Type == AccountTypeOAuth {
		reqOpts.BrowserImpersonation = UpstreamBrowserImpersonationChrome
	}
	reqCtx := WithUpstreamRequestOptions(ctx, reqOpts)

	upstreamReq, err := s.buildTranscriptionUpstreamRequest(reqCtx, c, account, reqBody, reqContentType, token)
	if err != nil {
		return nil, err
	}

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}

	resp, err := s.httpUpstream.Do(upstreamReq, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		safeErr := sanitizeUpstreamErrorMessage(err.Error())
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: 0,
			Kind:               "failover",
			Message:            safeErr,
		})
		return nil, &UpstreamFailoverError{StatusCode: http.StatusBadGateway}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		return s.handleAudioTranscriptionErrorResponse(ctx, c, account, resp, respBody)
	}

	if isEventStreamResponse(resp.Header) {
		outputTokens, streamErr := s.forwardAudioTranscriptionStream(c, resp)
		if streamErr != nil {
			return nil, streamErr
		}
		return &OpenAIAudioTranscriptionForwardResult{
			RequestID:    extractOpenAIRequestIDHeader(resp.Header),
			Duration:     time.Since(startTime),
			OutputTokens: outputTokens,
		}, nil
	}

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	ensureOpenAIRequestIDHeader(c.Writer.Header(), resp.Header)
	c.Status(resp.StatusCode)
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read transcription response: %w", err)
	}
	outputTokens := parseTranscriptionOutputTokens(respBody)
	if _, err := c.Writer.Write(respBody); err != nil {
		return nil, fmt.Errorf("copy transcription response: %w", err)
	}

	return &OpenAIAudioTranscriptionForwardResult{
		RequestID:    extractOpenAIRequestIDHeader(resp.Header),
		Duration:     time.Since(startTime),
		OutputTokens: outputTokens,
	}, nil
}

func (s *OpenAIGatewayService) forwardAudioTranscriptionStream(c *gin.Context, resp *http.Response) (int, error) {
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	ensureOpenAIRequestIDHeader(c.Writer.Header(), resp.Header)
	if c.Writer.Header().Get("Content-Type") == "" {
		c.Header("Content-Type", "text/event-stream")
	}
	c.Status(resp.StatusCode)
	flusher, _ := c.Writer.(http.Flusher)

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 4096), transcriptionSSELineBufferSize)

	var textBuilder strings.Builder
	finalText := ""

	for scanner.Scan() {
		line := scanner.Text()
		if _, err := c.Writer.Write([]byte(line)); err != nil {
			return 0, fmt.Errorf("write transcription chunk: %w", err)
		}
		if _, err := c.Writer.Write([]byte{'\n'}); err != nil {
			return 0, fmt.Errorf("write transcription chunk newline: %w", err)
		}
		if flusher != nil {
			flusher.Flush()
		}
		parseTranscriptionSSELine(line, &textBuilder, &finalText)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("scan transcription stream: %w", err)
	}

	finalString := finalText
	if finalString == "" {
		finalString = textBuilder.String()
	}
	return estimateTranscriptionOutputTokens(finalString), nil
}

func (s *OpenAIGatewayService) buildTranscriptionUpstreamRequest(ctx context.Context, c *gin.Context, account *Account, body []byte, contentType, token string) (*http.Request, error) {
	if account.Type != AccountTypeOAuth {
		// API-key path is intentionally not supported here — only the
		// Codex/ChatGPT OAuth subscription is wired up.
		return nil, errors.New("audio transcription is only supported on OpenAI OAuth accounts")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(account.GetBaseURL()), "/")
	if baseURL == "" {
		baseURL = "https://chatgpt.com"
	}
	targetURL := baseURL + chatGPTTranscriptionPath

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build transcription upstream request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "text/event-stream, application/json")
	// Force a Codex CLI user-agent on the OAuth path: ChatGPT 5xx-s if the
	// request looks like a generic Go client.
	req.Header.Set("User-Agent", codexCLITranscriptionUserAgent)
	req.Header.Set("originator", "codex_cli_rs")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Host = "chatgpt.com"
	if chatgptAccountID := account.GetChatGPTAccountID(); chatgptAccountID != "" {
		req.Header.Set("chatgpt-account-id", chatgptAccountID)
	}
	if customUA := account.GetOpenAIUserAgent(); customUA != "" {
		req.Header.Set("User-Agent", customUA)
	}
	return req, nil
}

const codexCLITranscriptionUserAgent = "codex_cli_rs/0.30.0 (Linux 6.8.0; x86_64) codex_cli_rs"

func convertTranscriptionMultipartForOAuth(body []byte, contentType string) ([]byte, string, error) {
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
	hasFile := false

	for {
		part, err := reader.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, "", err
		}

		formName := part.FormName()
		// Codex's ChatGPT OAuth transcription endpoint only sends the file
		// part. Keep public OpenAI-compatible fields at the gateway boundary
		// and do not forward them upstream.
		if formName != "file" {
			_, _ = io.Copy(io.Discard, part)
			_ = part.Close()
			continue
		}
		header := mimeheader.Clone(part.Header)
		if formName == "file" {
			hasFile = true
			if strings.TrimSpace(part.FileName()) == "" {
				header.Set("Content-Disposition", `form-data; name="file"; filename="audio.wav"`)
			}
			if strings.TrimSpace(header.Get("Content-Type")) == "" {
				header.Set("Content-Type", "application/octet-stream")
			}
		}

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
	if !hasFile {
		_ = writer.Close()
		return nil, "", errors.New("multipart body missing file field")
	}

	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), writer.FormDataContentType(), nil
}

// parseTranscriptionSSELine accumulates partial text deltas and captures the
// final completed text from the OpenAI Audio API stream events.
func parseTranscriptionSSELine(line string, textBuilder *strings.Builder, finalText *string) {
	line = strings.TrimSpace(line)
	const prefix = "data:"
	if !strings.HasPrefix(line, prefix) {
		return
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	if payload == "" || payload == "[DONE]" {
		return
	}
	var event struct {
		Type  string `json:"type"`
		Delta string `json:"delta"`
		Text  string `json:"text"`
	}
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return
	}
	if textBuilder != nil && strings.TrimSpace(event.Delta) != "" {
		_, _ = textBuilder.WriteString(event.Delta)
	}
	if finalText != nil && strings.TrimSpace(event.Text) != "" {
		*finalText = event.Text
	}
}

func parseTranscriptionOutputTokens(body []byte) int {
	if len(body) == 0 {
		return 0
	}
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0
	}
	return estimateTranscriptionOutputTokens(payload.Text)
}

// estimateTranscriptionOutputTokens approximates output tokens from text.
// Keep the previous Clawdi transcription billing heuristic: text output only,
// using the common four-runes-per-token estimate.
func estimateTranscriptionOutputTokens(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	runes := 0
	for range text {
		runes++
	}
	return (runes + 3) / 4
}

func (s *OpenAIGatewayService) handleAudioTranscriptionErrorResponse(ctx context.Context, c *gin.Context, account *Account, resp *http.Response, body []byte) (*OpenAIAudioTranscriptionForwardResult, error) {
	upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(body))
	upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
	upstreamDetail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		upstreamDetail = truncateString(string(body), maxBytes)
	}

	if s.shouldFailoverOpenAIUpstreamResponse(resp.StatusCode, upstreamMsg, body) {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:             account.Platform,
			AccountID:            account.ID,
			AccountName:          account.Name,
			UpstreamStatusCode:   resp.StatusCode,
			UpstreamRequestID:    extractOpenAIRequestIDHeader(resp.Header),
			Kind:                 "failover",
			Message:              upstreamMsg,
			Detail:               upstreamDetail,
			UpstreamResponseBody: upstreamDetail,
		})
		if s.rateLimitService != nil {
			_ = s.rateLimitService.HandleUpstreamError(ctx, account, resp.StatusCode, resp.Header, body)
		}
		return nil, &UpstreamFailoverError{
			StatusCode:             resp.StatusCode,
			ResponseBody:           body,
			ResponseHeaders:        resp.Header.Clone(),
			RetryableOnSameAccount: account.IsPoolMode() && (isPoolModeRetryableStatus(resp.StatusCode) || isOpenAITransientProcessingError(resp.StatusCode, upstreamMsg, body)),
		}
	}

	setOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)

	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:             account.Platform,
		AccountID:            account.ID,
		AccountName:          account.Name,
		UpstreamStatusCode:   resp.StatusCode,
		UpstreamRequestID:    extractOpenAIRequestIDHeader(resp.Header),
		Kind:                 "http_error",
		Message:              upstreamMsg,
		Detail:               upstreamDetail,
		UpstreamResponseBody: upstreamDetail,
	})

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	ensureOpenAIRequestIDHeader(c.Writer.Header(), resp.Header)
	c.Status(resp.StatusCode)
	if len(body) > 0 {
		if _, err := c.Writer.Write(body); err != nil {
			return nil, fmt.Errorf("write transcription error body: %w", err)
		}
	} else {
		c.JSON(http.StatusBadGateway, gin.H{
			"error": gin.H{
				"type":    "upstream_error",
				"message": "Upstream transcription failed",
			},
		})
	}

	if upstreamMsg == "" {
		return nil, fmt.Errorf("upstream transcription error: %d", resp.StatusCode)
	}
	return nil, fmt.Errorf("upstream transcription error: %d message=%s", resp.StatusCode, upstreamMsg)
}

func extractOpenAIRequestIDHeader(header http.Header) string {
	for _, name := range []string{"x-request-id", "openai-request-id", "x-openai-request-id", "x-oai-request-id"} {
		if v := strings.TrimSpace(header.Get(name)); v != "" {
			return v
		}
	}
	return ""
}

func ensureOpenAIRequestIDHeader(dst http.Header, src http.Header) {
	if dst == nil || src == nil {
		return
	}
	if v := dst.Get("x-request-id"); v != "" {
		return
	}
	if requestID := extractOpenAIRequestIDHeader(src); requestID != "" {
		dst.Set("x-request-id", requestID)
	}
}
