package service

import (
	"context"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

type upstreamRequestOptionsKey struct{}

// UpstreamBrowserImpersonation selects a browser-like client profile for
// upstream paths that are sensitive to non-browser fingerprints (notably
// ChatGPT/Codex OAuth audio transcription).
type UpstreamBrowserImpersonation string

const (
	UpstreamBrowserImpersonationNone   UpstreamBrowserImpersonation = ""
	UpstreamBrowserImpersonationChrome UpstreamBrowserImpersonation = "chrome"
)

// UpstreamRequestOptions are per-request transport hints attached via context
// so the transport layer can pick up the request-scoped overrides without
// changing the HTTPUpstream interface signature.
type UpstreamRequestOptions struct {
	// MinTLSVersion is request-scoped transport minimum TLS version (0 = default).
	MinTLSVersion uint16
	// BrowserImpersonation enables a browser-like upstream client.
	BrowserImpersonation UpstreamBrowserImpersonation
}

// WithUpstreamRequestOptions attaches options to the request context.
func WithUpstreamRequestOptions(ctx context.Context, opts UpstreamRequestOptions) context.Context {
	return context.WithValue(ctx, upstreamRequestOptionsKey{}, opts)
}

// GetUpstreamRequestOptions retrieves request-scoped upstream options.
func GetUpstreamRequestOptions(ctx context.Context) (UpstreamRequestOptions, bool) {
	if ctx == nil {
		return UpstreamRequestOptions{}, false
	}
	opts, ok := ctx.Value(upstreamRequestOptionsKey{}).(UpstreamRequestOptions)
	return opts, ok
}

// HTTPUpstream 上游 HTTP 请求接口
// 用于向上游 API（Claude、OpenAI、Gemini 等）发送请求
type HTTPUpstream interface {
	// Do 执行 HTTP 请求（不启用 TLS 指纹）
	Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error)

	// DoWithTLS 执行带 TLS 指纹伪装的 HTTP 请求
	//
	// profile 参数:
	//   - nil: 不启用 TLS 指纹，行为与 Do 方法相同
	//   - non-nil: 使用指定的 Profile 进行 TLS 指纹伪装
	//
	// Profile 由调用方通过 TLSFingerprintProfileService 解析后传入，
	// 支持按账号绑定的数据库 profile 或内置默认 profile。
	DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error)
}
