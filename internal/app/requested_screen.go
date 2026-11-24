package app

import (
	"context"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// requestedScreen enlarges only explicit screen requests, not automatic
// snapshots or background capture.
func requestedScreen(ctx context.Context, herdr domain.HerdrGateway, pane string, lines int) (domain.Screen, error) {
	if expanded, ok := herdr.(interface {
		ReadExpandedScreen(context.Context, string, int) (domain.Screen, error)
	}); ok {
		return expanded.ReadExpandedScreen(ctx, pane, lines)
	}
	return herdr.ReadScreen(ctx, pane, domain.ScreenVisible, lines)
}
