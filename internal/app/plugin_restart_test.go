package app

import (
	"context"
	"errors"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestPluginRestartOwnerAcknowledgementAndOverlap(t *testing.T) {
	f := newBridgeFixture(t)
	var launches int
	f.in.pluginRestart = func(ctx context.Context) (<-chan error, error) {
		launches++
		if ctx.Err() != nil {
			t.Fatalf("launch context already cancelled: %v", ctx.Err())
		}
		calls := f.tg.Calls()
		if len(calls) != 1 || calls[0] != "send:0:Restarting the Telegram Agents daemon … Herdr and its agents will keep running.:reply=10" {
			t.Fatalf("launcher ran before acknowledgement: %q", calls)
		}
		return nil, nil
	}
	cmd := domain.GeneralCommand{MessageID: 10, FromID: 1, Text: "/restart", Role: domain.RoleOperator}
	if err := f.in.HandleGeneral(f.ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if err := f.in.HandlePluginRestart(f.ctx, domain.GeneralCommand{MessageID: 11, FromID: 1}); err != nil {
		t.Fatal(err)
	}
	if launches != 1 {
		t.Fatalf("launch count = %d, want exactly one", launches)
	}
	assertCallsEqual(t, f.tg,
		"send:0:Restarting the Telegram Agents daemon … Herdr and its agents will keep running.:reply=10",
		"send:0:⚠️ a Telegram Agents restart is already underway; wait for its startup notice:reply=11")
}

func TestPluginRestartUnauthorizedAndTopicRouting(t *testing.T) {
	f := newBridgeFixture(t)
	f.in.cfg.OperatorIDs = []int64{1, 2}
	var launches int
	f.in.pluginRestart = func(context.Context) (<-chan error, error) { launches++; return nil, nil }
	if err := f.in.HandlePluginRestart(f.ctx, domain.GeneralCommand{MessageID: 1, FromID: 2}); err != nil {
		t.Fatal(err)
	}
	if sent := f.tg.Sent(); len(sent) != 1 || sent[0].Text != "⚠️ only the bot owner can restart Telegram Agents" {
		t.Fatalf("unauthorized reply = %+v", sent)
	}
	f.add(t, "p1", "t1", "agent", domain.StatusIdle)
	if err := f.in.HandleTopic(f.ctx, topicMsg(101, 2, "/restart")); err != nil {
		t.Fatal(err)
	}
	if launches != 0 {
		t.Fatalf("plugin restart launched %d times", launches)
	}
	sent := f.tg.Sent()
	if len(sent) != 1 || sent[0].Text != "agent commands live in the agent's topic; /restart is available in General" || sent[0].ThreadID != 101 {
		t.Fatalf("unauthorized/topic replies = %+v", sent)
	}
}

func TestPluginRestartLaunchFailureIsReportedWithoutLocking(t *testing.T) {
	f := newBridgeFixture(t)
	launchErr := errors.New("spawn failed")
	launches := 0
	f.in.pluginRestart = func(context.Context) (<-chan error, error) { launches++; return nil, launchErr }
	cmd := domain.GeneralCommand{MessageID: 4, FromID: 1, Text: "/restart", Role: domain.RoleOperator}
	if err := f.in.HandleGeneral(f.ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if f.in.pluginRestartBusy {
		t.Fatal("failed launch left restart busy")
	}
	if err := f.in.HandleGeneral(f.ctx, domain.GeneralCommand{MessageID: 5, FromID: 1, Text: "/restart", Role: domain.RoleOperator}); err != nil {
		t.Fatal(err)
	}
	if launches != 2 {
		t.Fatalf("launch count after retry = %d, want 2", launches)
	}
	assertCallsEqual(t, f.tg,
		"send:0:Restarting the Telegram Agents daemon … Herdr and its agents will keep running.:reply=4",
		"send:0:⚠️ could not launch the daemon restart: spawn failed:reply=4",
		"send:0:Restarting the Telegram Agents daemon … Herdr and its agents will keep running.:reply=5",
		"send:0:⚠️ could not launch the daemon restart: spawn failed:reply=5")
}

func TestPluginRestartDoesNotLaunchWhenAcknowledgementFails(t *testing.T) {
	f := newBridgeFixture(t)
	launches := 0
	f.in.pluginRestart = func(context.Context) (<-chan error, error) { launches++; return nil, nil }
	f.tg.FailNext("send", errors.New("telegram unavailable"))
	err := f.in.HandlePluginRestart(f.ctx, domain.GeneralCommand{MessageID: 6, FromID: 1})
	if err == nil {
		t.Fatal("expected acknowledgement failure")
	}
	if launches != 0 || f.in.pluginRestartBusy {
		t.Fatalf("launches=%d busy=%v after failed acknowledgement", launches, f.in.pluginRestartBusy)
	}
}

func TestPluginRestartWatcherSuppressesFailureAfterShutdown(t *testing.T) {
	for _, test := range []struct {
		name      string
		cancel    bool
		helperErr error
	}{
		{name: "successful helper", helperErr: nil},
		{name: "daemon shutdown", cancel: true, helperErr: errors.New("exit status 9")},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error)
			reported := make(chan error, 1)
			finished := watchPluginRestart(parent, done, func() bool { return true }, func(err error) {
				reported <- err
			})
			if test.cancel {
				cancel()
			}
			// The unbuffered send synchronizes with the detached helper watcher;
			// finished synchronizes the assertion with its completion.
			done <- test.helperErr
			<-finished
			select {
			case err := <-reported:
				t.Fatalf("failure reported = %v, want no report", err)
			default:
			}
		})
	}
}
