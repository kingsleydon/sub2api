package routes

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newGatewayRoutesTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	RegisterGatewayRoutes(
		router,
		&handler.Handlers{
			Gateway:       &handler.GatewayHandler{},
			OpenAIGateway: &handler.OpenAIGatewayHandler{},
		},
		servermiddleware.APIKeyAuthMiddleware(func(c *gin.Context) {
			groupID := int64(1)
			c.Set(string(servermiddleware.ContextKeyAPIKey), &service.APIKey{
				GroupID: &groupID,
				Group:   &service.Group{Platform: service.PlatformOpenAI},
			})
			c.Next()
		}),
		nil,
		nil,
		nil,
		nil,
		&config.Config{},
	)

	return router
}

func TestGatewayRoutesOpenAIResponsesCompactPathIsRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter()

	for _, path := range []string{
		"/v1/responses/compact",
		"/responses/compact",
		"/backend-api/codex/responses",
		"/backend-api/codex/responses/compact",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"gpt-5"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should hit OpenAI responses handler", path)
	}
}

func TestGatewayRoutesOpenAIImagesPathsAreRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter()

	for _, path := range []string{
		"/v1/images/generations",
		"/v1/images/edits",
		"/images/generations",
		"/images/edits",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"gpt-image-2","prompt":"draw a cat"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should hit OpenAI images handler", path)
	}
}

func TestShouldRouteToOpenAI_PreservesGroupedPlatformRouting(t *testing.T) {
	groupID := int64(1)
	c := newRouteDecisionContext(t, &service.APIKey{
		GroupID: &groupID,
		Group:   &service.Group{Platform: service.PlatformAnthropic},
	}, `{"model":"gpt-5.5"}`)

	require.False(t, shouldRouteToOpenAI(c, legacyOpenAIByModel))

	c = newRouteDecisionContext(t, &service.APIKey{
		GroupID: &groupID,
		Group:   &service.Group{Platform: service.PlatformOpenAI},
	}, `{"model":"kimi-for-coding"}`)

	require.True(t, shouldRouteToOpenAI(c, legacyOpenAIByModel))
}

func TestShouldRouteToOpenAI_LegacyUngroupedKeyRoutesByModel(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
		mode legacyOpenAICompatMode
	}{
		{
			name: "gpt model uses openai gateway",
			body: `{"model":"gpt-5.5"}`,
			want: true,
			mode: legacyOpenAIByModel,
		},
		{
			name: "provider prefixed gpt model uses openai gateway",
			body: `{"model":"openai-codex/gpt-5.4-mini"}`,
			want: true,
			mode: legacyOpenAIByModel,
		},
		{
			name: "kimi model stays on anthropic gateway",
			body: `{"model":"kimi-for-coding"}`,
			want: false,
			mode: legacyOpenAIByModel,
		},
		{
			name: "claude model stays on anthropic gateway",
			body: `{"model":"claude-sonnet-4-5"}`,
			want: false,
			mode: legacyOpenAIByModel,
		},
		{
			name: "codex route without model is openai",
			body: `{}`,
			want: true,
			mode: legacyOpenAIAlways,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newRouteDecisionContext(t, &service.APIKey{}, tt.body)
			require.Equal(t, tt.want, shouldRouteToOpenAI(c, tt.mode))

			got, err := io.ReadAll(c.Request.Body)
			require.NoError(t, err)
			require.Equal(t, tt.body, string(got), "route decision must not consume request body")
		})
	}
}

func newRouteDecisionContext(t *testing.T, apiKey *service.APIKey, body string) *gin.Context {
	t.Helper()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	c.Set(string(servermiddleware.ContextKeyAPIKey), apiKey)
	return c
}
