package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestHerdrServerControlIsOwnerOnlyAndReportsResult(t *testing.T) {
	f := newBridgeFixture(t)
	f.in.cfg.OperatorIDs = []int64{1, 2}
	var gotStop bool
	f.in.herdrServer = func(_ context.Context, stop bool) error {
		gotStop = stop
		return nil
	}

	if err := f.in.HandleHerdrServer(f.ctx, domain.GeneralCommand{MessageID: 4, FromID: 2}, true); err != nil {
		t.Fatal(err)
	}
	if len(f.tg.Sent()) != 1 || f.tg.Sent()[0].Text != "⚠️ only the bot owner can control the Herdr server" {
		t.Fatalf("non-owner reply = %+v", f.tg.Sent())
	}

	if err := f.in.HandleHerdrServer(f.ctx, domain.GeneralCommand{MessageID: 5, FromID: 1}, true); err != nil {
		t.Fatal(err)
	}
	if !gotStop {
		t.Fatal("owner request did not stop the server")
	}
	sent := f.tg.Sent()
	if len(sent) != 3 || sent[1].Text != "Stopping the configured default Herdr server …" || !strings.HasPrefix(sent[2].Text, "✅ ") {
		t.Fatalf("owner stop replies = %+v", sent)
	}
}

func TestHerdrServerControlReportsUnavailableAndFailure(t *testing.T) {
	f := newBridgeFixture(t)
	f.in.cfg.OperatorIDs = []int64{1}
	cmd := domain.GeneralCommand{MessageID: 8, FromID: 1}
	if err := f.in.HandleHerdrServer(f.ctx, cmd, false); err != nil {
		t.Fatal(err)
	}
	if got := f.tg.Sent()[0].Text; got != "⚠️ Herdr server control is unavailable in this build" {
		t.Fatalf("unavailable reply = %q", got)
	}

	f.tg.Reset()
	f.in.herdrServer = func(context.Context, bool) error { return errors.New("permission denied") }
	if err := f.in.HandleHerdrServer(f.ctx, cmd, false); err != nil {
		t.Fatal(err)
	}
	sent := f.tg.Sent()
	if len(sent) != 2 || sent[1].Text != "⚠️ Herdr server control failed: permission denied" {
		t.Fatalf("failure replies = %+v", sent)
	}
}

func TestHerdrServerStartRunsOnlyAfterOwnerAcknowledgement(t *testing.T) {
	f := newBridgeFixture(t)
	f.in.cfg.OperatorIDs = []int64{1}
	var called bool
	f.in.herdrServer = func(_ context.Context, stop bool) error {
		if stop {
			t.Error("start command requested a stop")
		}
		if got := len(f.tg.Sent()); got != 1 {
			t.Errorf("server start began after %d Telegram replies, want acknowledgement first", got)
		}
		called = true
		return nil
	}
	if err := f.in.HandleHerdrServer(f.ctx, domain.GeneralCommand{MessageID: 12, FromID: 1}, false); err != nil {
		t.Fatal(err)
	}
	sent := f.tg.Sent()
	if !called || len(sent) != 2 || sent[0].Text != "Starting the configured default Herdr server …" || !strings.HasPrefix(sent[1].Text, "✅ ") {
		t.Fatalf("called=%v replies=%+v", called, sent)
	}
}
