package herdr

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/testkit"
)

func TestExpandedScreenRestoresLayout(t *testing.T) {
	for _, initialZoom := range []bool{false, true} {
		t.Run(map[bool]string{false: "split", true: "other pane zoomed"}[initialZoom], func(t *testing.T) {
			var mu sync.Mutex
			focus, zoom := "w1:p2", initialZoom
			s := testkit.NewNDJSONServer(t, nil)
			s.Handle("pane.layout", func(_ string, _ json.RawMessage) (any, *testkit.APIError) {
				mu.Lock()
				defer mu.Unlock()
				return map[string]any{"layout": map[string]any{"focused_pane_id": focus, "zoomed": zoom}}, nil
			})
			s.Handle("pane.zoom", func(_ string, raw json.RawMessage) (any, *testkit.APIError) {
				var p struct {
					PaneID string `json:"pane_id"`
					Mode   string `json:"mode"`
				}
				_ = json.Unmarshal(raw, &p)
				mu.Lock()
				defer mu.Unlock()
				if p.Mode == "on" {
					focus, zoom = p.PaneID, true
				} else {
					zoom = false
				}
				return map[string]any{"type": "pane_zoom"}, nil
			})
			s.Handle("agent.read", func(_ string, _ json.RawMessage) (any, *testkit.APIError) {
				mu.Lock()
				defer mu.Unlock()
				if focus != "w1:p1" || !zoom {
					t.Errorf("read without zoom: %s %v", focus, zoom)
				}
				return map[string]any{"read": map[string]any{"text": "expanded"}}, nil
			})
			g := newGateway(t, s)
			screen, err := g.ReadExpandedScreen(context.Background(), "w1:p1", 0)
			if err != nil || screen.Text != "expanded" {
				t.Fatalf("screen = %+v, err = %v", screen, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if focus != "w1:p2" || zoom != initialZoom {
				t.Fatalf("layout after read: %s %v", focus, zoom)
			}
		})
	}
}

func TestExpandedScreenRestoresAfterCancellation(t *testing.T) {
	var mu sync.Mutex
	zoom := false
	s := testkit.NewNDJSONServer(t, nil)
	s.Handle("pane.layout", func(_ string, _ json.RawMessage) (any, *testkit.APIError) {
		mu.Lock()
		defer mu.Unlock()
		return map[string]any{"layout": map[string]any{"focused_pane_id": "w1:p1", "zoomed": zoom}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Handle("pane.zoom", func(_ string, raw json.RawMessage) (any, *testkit.APIError) {
		var p struct {
			Mode string `json:"mode"`
		}
		_ = json.Unmarshal(raw, &p)
		mu.Lock()
		zoom = p.Mode == "on"
		mu.Unlock()
		if p.Mode == "on" {
			time.AfterFunc(30*time.Millisecond, cancel)
		}
		return map[string]any{"type": "pane_zoom"}, nil
	})
	g := newGateway(t, s)
	if _, err := g.ReadExpandedScreen(ctx, "w1:p1", 0); err == nil {
		t.Fatal("expected cancellation")
	}
	mu.Lock()
	defer mu.Unlock()
	if zoom {
		t.Fatal("zoom was not restored after cancellation")
	}
}

func TestExpandedScreenLeavesUserLayoutChangeAlone(t *testing.T) {
	var mu sync.Mutex
	focus, zoom := "w1:p1", false
	s := testkit.NewNDJSONServer(t, nil)
	s.Handle("pane.layout", func(_ string, _ json.RawMessage) (any, *testkit.APIError) {
		mu.Lock()
		defer mu.Unlock()
		return map[string]any{"layout": map[string]any{"focused_pane_id": focus, "zoomed": zoom}}, nil
	})
	s.Handle("pane.zoom", func(_ string, _ json.RawMessage) (any, *testkit.APIError) {
		mu.Lock()
		defer mu.Unlock()
		zoom = true
		return map[string]any{"type": "pane_zoom"}, nil
	})
	s.Handle("agent.read", func(_ string, _ json.RawMessage) (any, *testkit.APIError) {
		mu.Lock()
		focus, zoom = "w1:p2", true
		mu.Unlock()
		return map[string]any{"read": map[string]any{"text": "screen"}}, nil
	})
	g := newGateway(t, s)
	if _, err := g.ReadExpandedScreen(context.Background(), "w1:p1", 0); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if focus != "w1:p2" || !zoom {
		t.Fatalf("overwrote user's layout: %s %v", focus, zoom)
	}
}
