package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type recordingAudioHTTPUpstream struct {
	doFunc func(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error)
}

func (u *recordingAudioHTTPUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return u.doFunc(req, proxyURL, accountID, accountConcurrency)
}

func (u *recordingAudioHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, accountConcurrency)
}

func TestBuildTranscriptionUpstreamRequest_OAuthHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{}

	tests := []struct {
		name          string
		requestUA     string
		accountUA     string
		chatGPTAcctID string
		expectedUA    string
	}{
		{
			name:       "default ignores caller ua",
			requestUA:  "curl/8.7.1",
			expectedUA: codexCLITranscriptionUserAgent,
		},
		{
			name:       "default also normalizes codex caller ua",
			requestUA:  "codex_cli_rs/0.99.0 (Linux; x86_64)",
			expectedUA: codexCLITranscriptionUserAgent,
		},
		{
			name:          "account ua and chatgpt account id preserved",
			accountUA:     "my-openai-agent/1.0",
			chatGPTAcctID: "chatgpt-account-1",
			expectedUA:    "my-openai-agent/1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.requestUA != "" {
				c.Request.Header.Set("User-Agent", tt.requestUA)
			}

			credentials := map[string]any{}
			if tt.accountUA != "" {
				credentials["user_agent"] = tt.accountUA
			}
			if tt.chatGPTAcctID != "" {
				credentials["chatgpt_account_id"] = tt.chatGPTAcctID
			}
			account := &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Credentials: credentials,
			}

			req, err := svc.buildTranscriptionUpstreamRequest(
				context.Background(),
				c,
				account,
				[]byte("dummy"),
				"multipart/form-data; boundary=abc",
				"token",
			)
			require.NoError(t, err)
			require.Equal(t, tt.expectedUA, req.Header.Get("User-Agent"))
			require.Equal(t, "codex_cli_rs", req.Header.Get("originator"))
			require.Equal(t, "responses=experimental", req.Header.Get("OpenAI-Beta"))
			require.Equal(t, tt.chatGPTAcctID, req.Header.Get("chatgpt-account-id"))
		})
	}
}

func TestForwardAudioTranscription_OAuthUsesChromeImpersonation(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var seen UpstreamRequestOptions
	upstream := &recordingAudioHTTPUpstream{
		doFunc: func(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
			opts, ok := GetUpstreamRequestOptions(req.Context())
			require.True(t, ok)
			seen = opts
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"text":"hello"}`)),
			}, nil
		},
	}

	svc := &OpenAIGatewayService{httpUpstream: upstream}
	body, contentType := buildTranscriptionMultipart(t, "voice.ogg", map[string]string{
		"model": "gpt-4o-transcribe",
	})

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", contentType)

	account := &Account{
		ID:       4,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":       "token",
			"chatgpt_account_id": "chatgpt-account",
		},
	}

	_, err := svc.ForwardAudioTranscription(context.Background(), c, account, body, contentType)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), seen.MinTLSVersion)
	require.Equal(t, UpstreamBrowserImpersonationChrome, seen.BrowserImpersonation)
	require.Contains(t, rec.Body.String(), `"text":"hello"`)
}

func TestConvertTranscriptionMultipartForOAuth_PreservesFieldsAndNormalizesFile(t *testing.T) {
	body, contentType := buildTranscriptionMultipart(t, "", map[string]string{
		"model":           "gpt-4o-transcribe",
		"response_format": "json",
		"stream":          "true",
		"include[]":       "logprobs",
	})

	convertedBody, convertedType, err := convertTranscriptionMultipartForOAuth(body, contentType)
	require.NoError(t, err)

	_, params, err := mime.ParseMediaType(convertedType)
	require.NoError(t, err)
	reader := multipart.NewReader(bytes.NewReader(convertedBody), params["boundary"])

	gotFields := map[string][]string{}
	var gotFile []byte
	fileName := ""
	fileContentType := ""
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		v, err := io.ReadAll(part)
		require.NoError(t, err)
		if part.FormName() == "file" {
			gotFile = v
			fileName = part.FileName()
			fileContentType = part.Header.Get("Content-Type")
		} else {
			gotFields[part.FormName()] = append(gotFields[part.FormName()], string(v))
		}
		require.NoError(t, part.Close())
	}

	require.Equal(t, "abc123", string(gotFile))
	require.Equal(t, "audio.wav", fileName)
	require.Equal(t, "application/octet-stream", fileContentType)
	require.Equal(t, []string{"gpt-4o-transcribe"}, gotFields["model"])
	require.Equal(t, []string{"json"}, gotFields["response_format"])
	require.Equal(t, []string{"true"}, gotFields["stream"])
	require.Equal(t, []string{"logprobs"}, gotFields["include[]"])
}

func TestConvertTranscriptionMultipartForOAuth_RejectsMissingFile(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("model", "gpt-4o-transcribe"))
	require.NoError(t, writer.Close())

	_, _, err := convertTranscriptionMultipartForOAuth(body.Bytes(), writer.FormDataContentType())
	require.ErrorContains(t, err, "missing file field")
}

func TestForwardAudioTranscriptionStream_EstimatesTokensAndForwardsSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)

	sseBody := strings.Join([]string{
		`data: {"type":"transcript.text.delta","delta":"Hello "}`,
		"",
		`data: {"type":"transcript.text.done","text":"Hello world.","usage":{"type":"tokens","input_tokens":12,"output_tokens":3}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(sseBody)),
		Header: http.Header{
			"Content-Type":     []string{"text/event-stream"},
			"X-Oai-Request-Id": []string{"req-stream-oai-1"},
		},
	}

	outputTokens, err := svc.forwardAudioTranscriptionStream(c, resp)
	require.NoError(t, err)
	require.Equal(t, 3, outputTokens)
	require.Contains(t, rec.Body.String(), `"transcript.text.done"`)
	require.Equal(t, "req-stream-oai-1", rec.Header().Get("X-Request-Id"))
	require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
}

func TestParseTranscriptionOutputTokens_FromJSONText(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "ignores explicit usage",
			body: `{"text":"hello","usage":{"input_tokens":12,"output_tokens":3}}`,
			want: 2,
		},
		{
			name: "ignores duration usage",
			body: `{"text":"hello","usage":{"type":"duration","seconds":2.5}}`,
			want: 2,
		},
		{
			name: "text only",
			body: `{"text":"Hello, world."}`,
			want: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseTranscriptionOutputTokens([]byte(tt.body)))
		})
	}
}

func buildTranscriptionMultipart(t *testing.T, fileName string, fields map[string]string) ([]byte, string) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	var filePart io.Writer
	var err error
	if fileName == "" {
		filePart, err = writer.CreateFormField("file")
	} else {
		filePart, err = writer.CreateFormFile("file", fileName)
	}
	require.NoError(t, err)
	_, err = filePart.Write([]byte("abc123"))
	require.NoError(t, err)
	for key, value := range fields {
		require.NoError(t, writer.WriteField(key, value))
	}
	require.NoError(t, writer.Close())
	return body.Bytes(), writer.FormDataContentType()
}
