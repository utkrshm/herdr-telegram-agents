package app

import (
	"context"
	"strings"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

type blockingGuardTelegram struct {
	domain.TelegramGateway
	entered chan struct{}
	resume  chan struct{}
}

func (g *blockingGuardTelegram) SendGuard(ctx context.Context, out domain.Outgoing, guard domain.DispatchGuard) (int, error) {
	close(g.entered)
	<-g.resume
	call, release, err := guard(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return g.TelegramGateway.Send(call, out)
}

func TestOwnerShellRequiresIdleOpenCodeAndQuotes(t *testing.T) {
	for _, test := range []struct {
		name      string
		kind      string
		status    domain.Status
		screen    string
		command   string
		wantCalls int
	}{
		{"other agent", "claude", domain.StatusIdle, "prompt", `/shell "pwd"`, 0},
		{"busy", domain.OpenCodeKind, domain.StatusWorking, "prompt", `/shell "pwd"`, 0},
		{"blocked", domain.OpenCodeKind, domain.StatusBlocked, "prompt", `/shell "pwd"`, 0},
		{"dialog", domain.OpenCodeKind, domain.StatusIdle, "1. Continue\n2. Cancel", `/shell "pwd"`, 0},
		{"unquoted", domain.OpenCodeKind, domain.StatusIdle, "prompt", `/shell pwd`, 0},
		{"accepted", domain.OpenCodeKind, domain.StatusIdle, "prompt", `/shell "pwd"`, 1},
		{"done is ready", domain.OpenCodeKind, domain.StatusDone, "prompt", `/shell "pwd"`, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newBridgeFixture(t)
			a := f.add(t, "p1", "t1", "opencode", test.status)
			a.Kind = test.kind
			f.agents[a.Key] = a
			f.herdr.SetScreen("p1", test.screen)
			called := 0
			f.in.shell = func(_ context.Context, _ domain.Agent, command string, submitted func() error, _ func(domain.Screen) error) (string, error) {
				called++
				if command != "pwd" {
					t.Errorf("command = %q", command)
				}
				if err := submitted(); err != nil {
					t.Errorf("submission ack: %v", err)
				}
				return "/repo", nil
			}
			if err := f.in.HandleTopic(f.ctx, topicMsg(101, 12, test.command)); err != nil {
				t.Fatal(err)
			}
			if called != test.wantCalls {
				t.Fatalf("shell calls = %d want %d", called, test.wantCalls)
			}
			if test.wantCalls != 0 {
				calls := strings.Join(f.tg.Calls(), "\n")
				if !strings.Contains(calls, "/repo") || !strings.Contains(calls, "exit status is not exposed") {
					t.Fatalf("missing result disclaimer: %s", calls)
				}
			}
		})
	}
}

func TestOwnerShellResultPreservesLongOutputAsRedactedDocument(t *testing.T) {
	f := newBridgeFixture(t)
	agent := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
	agent.Kind = domain.OpenCodeKind
	f.agents[agent.Key] = agent
	generation, err := f.view.generation(agent.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	output := strings.Repeat("x", 5000)
	if err := f.in.shellFinished(f.ctx, shellResult{key: agent.Key, threadID: 101, messageID: 22, generation: generation, output: output}); err != nil {
		t.Fatal(err)
	}
	docs := f.tg.Documents()
	if len(docs) != 1 || len(docs[0].Data) <= 5000 || !strings.Contains(string(docs[0].Data), "exit status is not exposed") {
		t.Fatalf("document delivery lost output/disclaimer: %#v", docs)
	}
}

func TestShellStatusReadsOpenCodeScreenWhileWorkingWithoutInput(t *testing.T) {
	for _, status := range []domain.Status{domain.StatusIdle, domain.StatusWorking} {
		t.Run(string(status), func(t *testing.T) {
			f := newBridgeFixture(t)
			a := f.add(t, "pane", "terminal", "opencode", status)
			a.Kind = domain.OpenCodeKind
			f.agents[a.Key] = a
			f.herdr.SetScreen("pane", "visible secret=abc")
			started := make(chan func(context.Context) any, 1)
			f.in.asyncLong = func(run func(context.Context) any) { started <- run }
			if err := f.in.HandleTopic(f.ctx, topicMsg(101, 12, "/shell-status")); err != nil {
				t.Fatal(err)
			}
			job := <-started
			result := job(f.ctx)
			if err := f.in.asyncDone(f.ctx, result); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(f.tg.Calls(), "\n"), "visible screen snapshot") {
				t.Fatalf("snapshot missing: %v", f.tg.Calls())
			}
			if len(f.herdr.Keys()) != 0 || len(f.herdr.Texts()) != 0 || len(f.herdr.Prompts()) != 0 {
				t.Fatal("snapshot sent input to Herdr")
			}
		})
	}
}

func TestShellStatusRejectsOtherAgentAndDropsStaleGeneration(t *testing.T) {
	f := newBridgeFixture(t)
	a := f.add(t, "pane", "terminal", "claude", domain.StatusWorking)
	if err := f.in.HandleTopic(f.ctx, topicMsg(101, 12, "/shell-status")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(f.tg.Calls(), "\n"), "only for OpenCode") {
		t.Fatal("missing wrong-agent explanation")
	}
	a.Kind = domain.OpenCodeKind
	f.agents[a.Key] = a
	var run func(context.Context) any
	f.in.asyncLong = func(job func(context.Context) any) { run = job }
	if err := f.in.HandleTopic(f.ctx, topicMsg(101, 13, "/shell-status")); err != nil {
		t.Fatal(err)
	}
	result := run(f.ctx)
	f.view.invalidate(a.Key)
	before := len(f.tg.Calls())
	_ = f.in.asyncDone(f.ctx, result)
	if len(f.tg.Calls()) != before {
		t.Fatal("stale snapshot was dispatched")
	}
}

func TestShellStatusDropsReplyInvalidatedWhileDispatchIsBlocked(t *testing.T) {
	f := newBridgeFixture(t)
	a := f.add(t, "pane", "terminal", "opencode", domain.StatusWorking)
	a.Kind = domain.OpenCodeKind
	f.agents[a.Key] = a
	generation, err := f.view.generation(a.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	delayed := &blockingGuardTelegram{TelegramGateway: f.tg, entered: make(chan struct{}), resume: make(chan struct{})}
	f.in.tg = delayed
	done := make(chan error, 1)
	go func() {
		done <- f.in.shellStatusFinished(f.ctx, shellStatusResult{key: a.Key, threadID: 101, messageID: 77, generation: generation, screen: "still working"})
	}()
	<-delayed.entered
	f.view.invalidate(a.Key)
	close(delayed.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(f.tg.Calls()) != 0 {
		t.Fatalf("stale reply dispatched: %v", f.tg.Calls())
	}
}
