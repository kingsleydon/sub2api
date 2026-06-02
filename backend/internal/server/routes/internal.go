package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"

	"github.com/gin-gonic/gin"
)

// RegisterInternalRoutes registers internal API routes that use shared-secret
// authentication (not admin JWT). These are called by the Clawdi backend.
func RegisterInternalRoutes(
	v1 *gin.RouterGroup,
	h *handler.Handlers,
) {
	internal := v1.Group("/internal")
	{
		billing := internal.Group("/billing")
		{
			billing.GET("/usage-outbox", h.Admin.BillingSync.GetUsageOutbox)
			billing.POST("/usage-outbox/ack", h.Admin.BillingSync.AckUsageOutbox)
		}
	}
}
