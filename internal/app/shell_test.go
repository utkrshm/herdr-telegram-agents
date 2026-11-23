package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	telegramadapter "github.com/permgps/herdr-telegram-agents/internal/adapters/telegram"
	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

type blockingGuardTelegram struct {
	domain.TelegramGateway
	entered chan struct{}
	resume  chan struct{}
}

type pacedShellProgressTelegram struct {
	domain.TelegramGateway
	queue      *telegramadapter.Queue
	dispatched atomic.Int32
}

func (g *pacedShellProgressTelegram) SendGuard(ctx context.Context, _ domain.Outgoing, guard domain.DispatchGuard) (int, error) {
	err := g.queue.DoGuard(ctx, guard, func(context.Context) error {
		g.dispatched.Add(1)
		return nil
	})
	return 0, err
}

func (g *pacedShellProgressTelegram) SendDocumentGuard(ctx context.Context, _ domain.Document, guard domain.DispatchGuard) error {
	return g.queue.DoGuard(ctx, guard, func(context.Context) error {
		g.dispatched.Add(1)
		return nil
	})
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
			f.in.shell = func(_ context.Context, _ domain.Agent, command string, submitted func() error, _ func(context.Context, domain.Screen) error) (string, error) {
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

func TestShellFinishedReportsSanitizedFailureStage(t *testing.T) {
	f := newBridgeFixture(t)
	var logs bytes.Buffer
	f.in.log = slog.New(slog.NewJSONHandler(&logs, nil))
	agent := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
	agent.Kind = domain.OpenCodeKind
	f.agents[agent.Key] = agent
	generation, err := f.view.generation(agent.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	err = f.in.shellFinished(f.ctx, shellResult{
		key: agent.Key, threadID: 101, messageID: 22, generation: generation,
		err: domain.WithShellStage(domain.ShellStageTypedInput, errors.New("/private/path secret payload")),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(f.tg.Calls(), "\n")
	if !strings.Contains(got, "typed input") {
		t.Fatalf("missing typed-input stage: %s", got)
	}
	for _, forbidden := range []string{"/private/path", "secret payload"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("failure detail leaked in Telegram reply: %s", got)
		}
	}
	if !strings.Contains(logs.String(), `"stage":"typed-input"`) {
		t.Fatalf("stage missing from log: %s", logs.String())
	}
	for _, forbidden := range []string{"/private/path", "secret payload"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Fatalf("failure detail leaked in log: %s", logs.String())
		}
	}
}

func TestShellFinishedUnknownErrorKeepsGenericSafeReply(t *testing.T) {
	f := newBridgeFixture(t)
	agent := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
	agent.Kind = domain.OpenCodeKind
	f.agents[agent.Key] = agent
	generation, err := f.view.generation(agent.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.in.shellFinished(f.ctx, shellResult{
		key: agent.Key, threadID: 101, messageID: 22, generation: generation,
		err: errors.New("credential /private/path"),
	}); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(f.tg.Calls(), "\n")
	if !strings.Contains(got, "did not confirm command submission") || strings.Contains(got, "credential") {
		t.Fatalf("unsafe or unexpected unknown-error reply: %s", got)
	}
}

func TestShellFinishedLogsSafeStageButSuppressesStaleReply(t *testing.T) {
	f := newBridgeFixture(t)
	var logs bytes.Buffer
	f.in.log = slog.New(slog.NewJSONHandler(&logs, nil))
	agent := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
	agent.Kind = domain.OpenCodeKind
	f.agents[agent.Key] = agent
	generation, err := f.view.generation(agent.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	f.view.invalidate(agent.Key)
	if err := f.in.shellFinished(f.ctx, shellResult{
		key: agent.Key, threadID: 101, messageID: 22, generation: generation,
		err: domain.WithShellStage(domain.ShellStageDraftVerification, errors.New("secret command and path")),
	}); err != nil {
		t.Fatal(err)
	}
	if len(f.tg.Calls()) != 0 {
		t.Fatalf("stale shell result was replied to: %v", f.tg.Calls())
	}
	if !strings.Contains(logs.String(), `"stage":"draft-verification"`) {
		t.Fatalf("stale failure stage not logged: %s", logs.String())
	}
	for _, secret := range []string{"secret command", "path"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("failure details leaked in log: %s", logs.String())
		}
	}
}

func TestShellStageMessagesAreExactAndSafe(t *testing.T) {
	tests := []struct {
		stage domain.ShellStage
		want  string
	}{
		{domain.ShellStagePreflight, "preflight"},
		{domain.ShellStagePromptVerification, "safe prompt"},
		{domain.ShellStageShellModeKey, "shell-mode switch"},
		{domain.ShellStageShellModeCheck, "shell mode could not be verified"},
		{domain.ShellStageTypedInput, "submission is uncertain"},
		{domain.ShellStageDraftVerification, "Enter was not sent"},
		{domain.ShellStageEnter, "could not confirm Enter"},
		{domain.ShellStageRecordWait, "result is not available yet"},
		{domain.ShellStageRecordRead, "recorded result could not be verified"},
	}
	for _, test := range tests {
		t.Run(string(test.stage), func(t *testing.T) {
			got := shellStageMessage(test.stage)
			if !strings.Contains(got, test.want) {
				t.Fatalf("message %q does not identify %q", got, test.want)
			}
			for _, secret := range []string{"/private/path", "secret payload", "credential"} {
				if strings.Contains(got, secret) {
					t.Fatalf("stage reply leaked %q", secret)
				}
			}
		})
	}
	if got := shellStageMessage("unknown"); got != "" {
		t.Fatalf("unknown stage message = %q", got)
	}
}

func TestShellFinishedKeepsEnterFailureAmbiguous(t *testing.T) {
	f := newBridgeFixture(t)
	agent := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
	agent.Kind = domain.OpenCodeKind
	f.agents[agent.Key] = agent
	generation, err := f.view.generation(agent.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.in.shellFinished(f.ctx, shellResult{
		key: agent.Key, threadID: 101, messageID: 22, generation: generation,
		err: domain.WithShellStage(domain.ShellStageEnter, errors.New("secret payload")),
	}); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(f.tg.Calls(), "\n")
	if !strings.Contains(got, "submission is uncertain") || strings.Contains(got, "command was not sent") {
		t.Fatalf("Enter ambiguity not preserved: %s", got)
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

func TestQueuedShellProgressIsDroppedAfterOperationFinishes(t *testing.T) {
	f := newBridgeFixture(t)
	a := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
	a.Kind = domain.OpenCodeKind
	f.agents[a.Key] = a
	generation, err := f.view.generation(a.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	b := &Bridge{in: f.in, control: make(chan any, 1), fatal: make(chan error, 1), log: slog.New(slog.DiscardHandler)}
	fillerLifecycle := &shellLifecycle{}
	fillerLifecycle.finished.Store(true)
	b.control <- shellProgress{key: a.Key, threadID: 101, messageID: 1, generation: generation, lifecycle: fillerLifecycle}

	lifecycle := &shellLifecycle{}
	job := shellProgress{key: a.Key, threadID: 101, messageID: 2, generation: generation, screen: "stale", lifecycle: lifecycle}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan error, 1)
	go func() { accepted <- b.SubmitContext(ctx, job) }()
	lifecycle.finished.Store(true)
	b.handle(context.Background(), <-b.control)
	if err := <-accepted; err != nil {
		t.Fatalf("progress enqueue = %v", err)
	}
	b.handle(context.Background(), <-b.control)
	result := shellResult{key: a.Key, threadID: 101, messageID: 2, generation: generation, output: "final result", lifecycle: lifecycle}
	b.control <- result
	b.handle(context.Background(), <-b.control)
	calls := strings.Join(f.tg.Calls(), "\n")
	if strings.Contains(calls, "stale") {
		t.Fatalf("stale progress was delivered after completion: %s", calls)
	}
	if !strings.Contains(calls, "final result") {
		t.Fatalf("completed result was not delivered: %s", calls)
	}
	b.control <- shellProgress{key: a.Key, threadID: 101, messageID: 3, generation: generation, lifecycle: fillerLifecycle}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.SubmitContext(canceledCtx, job); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled progress submission = %v", err)
	}
	if len(b.control) != 1 {
		t.Fatalf("canceled progress entered full queue; length = %d", len(b.control))
	}
}

func TestShellProgressDispatchGuardRechecksAfterTelegramQueuePacing(t *testing.T) {
	for _, delivery := range []string{"text", "document"} {
		t.Run(delivery, func(t *testing.T) {
			f := newBridgeFixture(t)
			a := f.add(t, "pane", "terminal", "opencode", domain.StatusIdle)
			a.Kind = domain.OpenCodeKind
			f.agents[a.Key] = a
			generation, err := f.view.generation(a.Key, 101)
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			resume := make(chan struct{})
			queue := telegramadapter.NewQueue(slog.New(slog.DiscardHandler), telegramadapter.QueueConfig{
				MinGap: time.Second,
				Sleep: func(ctx context.Context, _ time.Duration) error {
					select {
					case <-entered:
					default:
						close(entered)
					}
					select {
					case <-resume:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			})
			queueCtx, stopQueue := context.WithCancel(context.Background())
			defer stopQueue()
			go queue.Run(queueCtx)
			if err := queue.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}

			lifecycle := &shellLifecycle{}
			gateway := &pacedShellProgressTelegram{TelegramGateway: f.tg, queue: queue}
			f.in.tg = gateway
			screenLength := 10
			if delivery == "document" {
				screenLength = 4000
			}
			done := make(chan error, 1)
			go func() {
				done <- f.in.shellProgressFinished(context.Background(), shellProgress{key: a.Key, threadID: 101, messageID: 9,
					generation: generation, screen: strings.Repeat("x", screenLength), lifecycle: lifecycle})
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("Telegram pacing did not pause dispatch")
			}
			lifecycle.finished.Store(true)
			close(resume)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("queued dispatch did not finish")
			}
			if gateway.dispatched.Load() != 0 {
				t.Fatalf("%s progress dispatched after operation completion", delivery)
			}
		})
	}
}
