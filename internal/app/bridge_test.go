package app

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
	"github.com/permgps/herdr-telegram-agents/internal/testkit"
)

type runningBridge struct {
	*bridgeFixture
	bridge *Bridge
	cancel context.CancelFunc
	done   chan struct{}
}

// reactionsOn switches the prompt reactions on for the running bridge,
// which is built without an options registry.
func (r *runningBridge) reactionsOn(t *testing.T) {
	t.Helper()
	r.bridge.out.reactions = func() bool { return true }
}

// newRunningBridge builds a Bridge over the fixture's fakes with a real
// registry and reconciler, and runs it until the test ends.
// setup runs against the bridge before Run starts.
func newRunningBridge(t *testing.T, setup ...func(*bridgeFixture, *Bridge)) *runningBridge {
	t.Helper()
	f := newBridgeFixture(t)
	cfg := domain.Config{ChatID: -1001234567890, BotUsername: "agents_bot"}
	registry := NewRegistry(f.herdr, f.clock, nil)
	reconciler := NewReconciler(f.tg, f.herdr, testkit.NewMemMappingStore(), f.mapping, nil, f.clock, nil)
	b := NewBridge(cfg, f.herdr, f.tg, registry, reconciler, f.capture, nil, Services{Git: f.git, Inbox: f.inbox}, f.clock, nil)
	// The fixture's agent map stands in for the registry so tests control
	// statuses directly.
	b.out.agents = f.out.agents
	b.out.live = f.out.live
	b.in.agents = f.in.agents
	b.in.live = f.in.live
	b.out.topics, b.in.topics = f.view, f.view
	for _, fn := range setup {
		fn(f, b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return &runningBridge{bridgeFixture: f, bridge: b, cancel: cancel, done: done}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestBridgeRunsJobsInOrder(t *testing.T) {
	r := newRunningBridge(t)
	r.reactionsOn(t)
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.bridge.Submit(topicMsg(101, 1, "first"))
	r.bridge.Submit(topicMsg(101, 2, "second"))
	r.bridge.Submit(domain.GeneralCommand{MessageID: 3, FromID: 1, Text: "/help"})
	// Both accepted prompts are acknowledged and screened before /help.
	waitUntil(t, "three jobs", func() bool { return len(r.tg.Calls()) == 7 })
	if p := r.herdr.Prompts(); len(p) != 2 || p[0] != "p1: first" || p[1] != "p1: second" {
		t.Fatalf("Prompts = %v", p)
	}
	calls := r.tg.Calls()
	if len(calls) != 7 || calls[len(calls)-1] != "send:0:"+helpText+":reply=3" {
		t.Fatalf("prompt jobs and help ordering = %q", calls)
	}
	counts := map[string]int{}
	for _, call := range calls {
		counts[call]++
	}
	if counts["react:101:1:👀"] != 1 || counts["react:101:2:👀"] != 1 || counts["send:101:✅ Prompt sent.:reply=1"] != 1 || counts["send:101:✅ Prompt sent.:reply=2"] != 1 || counts["send:101:screen:reply=1"] != 1 || counts["send:101:screen:reply=2"] != 1 {
		t.Fatalf("prompt reactions/screens missing: %q", calls)
	}
	firstScreen, secondAck := -1, -1
	for index, call := range calls {
		if call == "send:101:screen:reply=1" {
			firstScreen = index
		}
		if call == "send:101:✅ Prompt sent.:reply=2" {
			secondAck = index
		}
	}
	if firstScreen < 0 || secondAck < firstScreen {
		t.Fatalf("same-topic notices reordered: %q", calls)
	}
}

func TestTransferWorkerTimeoutReportsPartialResults(t *testing.T) {
	r := newRunningBridge(t, func(_ *bridgeFixture, b *Bridge) { b.transferTimeout = 75 * time.Millisecond })
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	dir := t.TempDir()
	first, second := dir+"/first.bin", dir+"/second.bin"
	if err := os.WriteFile(first, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingTransferTelegram{TelegramGateway: r.bridge.in.tg, started: make(chan struct{}, 1), release: make(chan struct{}), blockOn: 2}
	r.bridge.in.tg = blocked
	r.bridge.Submit(topicMsg(101, 501, "/transfer "+first+" "+second+" "+dir+"/never.bin"))
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("second upload did not start")
	}
	waitUntil(t, "partial transfer result after worker timeout", func() bool {
		for _, sent := range r.tg.Sent() {
			if sent.ReplyTo == 501 && strings.Contains(sent.Text, "✅ sent first.bin") && strings.Contains(sent.Text, "outcome unknown") && strings.Contains(sent.Text, "timed out") {
				return true
			}
		}
		return false
	})
	if len(r.tg.Documents()) != 1 || r.tg.Documents()[0].ThreadID != 101 {
		t.Fatalf("successful upload routing = %+v", r.tg.Documents())
	}
}

func TestTransferWorkerStopsOnBridgeShutdown(t *testing.T) {
	r := newRunningBridge(t)
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	path := t.TempDir() + "/blocked.bin"
	if err := os.WriteFile(path, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingTransferTelegram{TelegramGateway: r.bridge.in.tg, started: make(chan struct{}, 1), release: make(chan struct{}), blockOn: 1}
	r.bridge.in.tg = blocked
	r.bridge.Submit(topicMsg(101, 502, "/transfer "+path))
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("bridge did not stop after cancelling upload")
	}
}

func TestPromptScreenCaptureDoesNotBlockBridgeControls(t *testing.T) {
	r := newRunningBridge(t, func(f *bridgeFixture, _ *Bridge) {
		f.herdr.SetReadDelay(250 * time.Millisecond)
	})
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.bridge.Submit(topicMsg(101, 91, "start work"))
	r.bridge.Submit(topicMsg(101, 92, "/focus"))
	waitUntil(t, "focus while screen capture is delayed", func() bool { return len(r.herdr.Focused()) == 1 })
	waitUntil(t, "prompt acknowledgement", func() bool {
		for _, call := range r.tg.Calls() {
			if call == "send:101:✅ Prompt sent.:reply=91" {
				return true
			}
		}
		return false
	})
	if calls := r.tg.Calls(); len(calls) < 1 {
		t.Fatalf("prompt acknowledgement did not precede capture: %q", calls)
	}
}

func TestPromptNoticeFatalTelegramErrorReachesBridgeFatal(t *testing.T) {
	r := newRunningBridge(t)
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.tg.FailNext("send", domain.ErrForbidden)
	r.bridge.Submit(topicMsg(101, 93, "start work"))
	select {
	case err := <-r.bridge.Fatal():
		if !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("Fatal = %v, want forbidden", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fatal prompt acknowledgement error was not forwarded")
	}
}

func TestQueuedPromptNoticeIsDroppedAfterTopicGenerationChanges(t *testing.T) {
	r := newRunningBridge(t, func(f *bridgeFixture, _ *Bridge) {
		f.herdr.SetReadDelay(200 * time.Millisecond)
	})
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.bridge.Submit(topicMsg(101, 94, "first"))
	waitUntil(t, "first acknowledgement", func() bool {
		for _, call := range r.tg.Calls() {
			if call == "send:101:✅ Prompt sent.:reply=94" {
				return true
			}
		}
		return false
	})
	r.bridge.Submit(topicMsg(101, 95, "second"))
	waitUntil(t, "second prompt accepted", func() bool { return len(r.herdr.Prompts()) == 2 })
	r.bridge.out.topics.invalidate(a.Key)
	time.Sleep(300 * time.Millisecond)
	for _, call := range r.tg.Calls() {
		if call == "send:101:✅ Prompt sent.:reply=95" || call == "send:101:screen:reply=95" {
			t.Fatalf("stale prompt notice reached reassigned topic: %q", r.tg.Calls())
		}
	}
}

func TestBridgeSettleTimerFiresThroughRun(t *testing.T) {
	r := newRunningBridge(t)
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusWorking)
	r.herdr.SetScreen("p1", "question?")
	r.bridge.Submit(AgentEvent{Kind: AgentChanged, Agent: r.setStatus(a, domain.StatusBlocked)})
	waitUntil(t, "settle timer armed", func() bool { return r.clock.Pending() == 1 })
	if n := len(r.tg.Sent()); n != 0 {
		t.Fatalf("posted before settle: %d", n)
	}
	r.clock.Advance(screenSettle)
	waitUntil(t, "screen post", func() bool { return len(r.tg.Sent()) == 1 })
	if sent := r.tg.Sent(); sent[0].Text != "question?" || !sent[0].Notify {
		t.Fatalf("Sent = %+v", sent)
	}
}

func TestBridgeServesCommandFollowUp(t *testing.T) {
	r := newRunningBridge(t)
	r.claude(r.add(t, "p1", "t1", "reviewer", domain.StatusIdle))
	r.herdr.SetScreen("p1", overlayScreen)
	r.bridge.Submit(topicMsg(101, 7, "/usage"))
	waitUntil(t, "command typed", func() bool { return len(r.herdr.Prompts()) == 1 })
	waitUntil(t, "follow-up timer armed", func() bool { return r.clock.Pending() == 1 })
	if n := len(r.tg.Sent()); n != 0 {
		t.Fatalf("posted before settle: %d", n)
	}
	r.clock.Advance(commandSettle)
	waitUntil(t, "screen post and esc", func() bool { return len(r.tg.Sent()) == 1 && len(r.herdr.Keys()) == 1 })
	if sent := r.tg.Sent(); sent[0].ReplyTo != 7 || !sent[0].Code || !strings.HasPrefix(sent[0].Text, "   Settings") {
		t.Fatalf("Sent = %+v", sent)
	}
}

func TestBridgeStateIngressWaitsForCapacityAndCancel(t *testing.T) {
	f := newBridgeFixture(t)
	b := &Bridge{out: f.out, in: f.in, jobs: make(chan any, 1), control: make(chan any, 1), fatal: make(chan error, 1), log: f.out.log, CallTimeout: time.Second}
	b.Submit(AgentEvent{Kind: AgentChanged})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- b.SubmitContext(ctx, AgentEvent{Kind: AgentGone}) }()
	select {
	case err := <-result:
		t.Fatalf("state returned before capacity: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled state = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled state producer remained blocked")
	}
	if b.Dropped() != 0 || len(b.jobs) != 1 {
		t.Fatalf("state lost: dropped=%d queued=%d", b.Dropped(), len(b.jobs))
	}
}

func TestBridgeControlIngressWaitsForCapacityAndCancel(t *testing.T) {
	f := newBridgeFixture(t)
	b := &Bridge{out: f.out, in: f.in, jobs: make(chan any, 1), control: make(chan any, 1), fatal: make(chan error, 1), log: f.out.log, CallTimeout: time.Second}
	b.Submit(topicMsg(101, 1, "first"))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- b.SubmitContext(ctx, topicMsg(101, 2, "second")) }()
	select {
	case err := <-result:
		t.Fatalf("control work returned before capacity was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	<-b.control
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("SubmitContext = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("control work did not enter queue")
	}
	if b.Dropped() != 0 {
		t.Fatalf("control work dropped: %d", b.Dropped())
	}
	result = make(chan error, 1)
	go func() { result <- b.SubmitContext(ctx, topicMsg(101, 3, "third")) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled SubmitContext = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled producer remained blocked")
	}
}

func TestBridgeControlTypesNeverCountAsRecoverableDrops(t *testing.T) {
	types := []any{
		topicMsg(101, 1, "hello"),
		domain.TopicAttachment{},
		domain.ButtonPressed{},
		domain.GeneralCommand{},
		startResult{},
		inboxResult{},
		domain.StrangerSeen{},
	}
	for _, job := range types {
		f := newBridgeFixture(t)
		b := &Bridge{out: f.out, in: f.in, jobs: make(chan any, 1), control: make(chan any, 1), fatal: make(chan error, 1), log: f.out.log}
		b.control <- presenceAway{}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- b.SubmitContext(ctx, job) }()
		select {
		case err := <-result:
			t.Fatalf("%T returned before capacity: %v", job, err)
		case <-time.After(5 * time.Millisecond):
		}
		<-b.control
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%T: %v", job, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%T did not enter control queue", job)
		}
		cancel()
		if b.Dropped() != 0 {
			t.Fatalf("%T counted as dropped", job)
		}
	}
}

func TestBridgeAcceptsStrangerSeen(t *testing.T) {
	r := newRunningBridge(t)
	r.bridge.Submit(domain.StrangerSeen{FromID: 777, Name: "Ann", At: r.clock.Now()})
	waitUntil(t, "stranger observation", func() bool { return r.bridge.Handled() == 1 })
	if len(r.bridge.in.strangers) != 1 {
		t.Fatalf("strangers = %d", len(r.bridge.in.strangers))
	}
}

func TestBridgeReportsFatalOnce(t *testing.T) {
	r := newRunningBridge(t)
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.tg.FailNext("send", domain.ErrForbidden)
	r.bridge.Submit(topicMsg(101, 1, "/help"))
	select {
	case err := <-r.bridge.Fatal():
		if !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("Fatal = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fatal error not reported")
	}
	// A non-fatal failure afterwards is absorbed and the loop keeps serving.
	r.tg.FailNext("send", domain.ErrTopicGone)
	r.bridge.Submit(topicMsg(101, 2, "/help"))
	r.bridge.Submit(topicMsg(101, 3, "/help"))
	waitUntil(t, "later jobs served", func() bool { return len(r.tg.Calls()) == 3 })
	select {
	case err := <-r.bridge.Fatal():
		t.Fatalf("second fatal reported: %v", err)
	default:
	}
}

// TestBridgeFullStateQueueDrains fills the queue before Run starts and checks
// that an additional state transition waits until Run can accept it.
func TestBridgeFullStateQueueDrains(t *testing.T) {
	f := newBridgeFixture(t)
	cfg := domain.Config{ChatID: -1001234567890, BotUsername: "agents_bot"}
	registry := NewRegistry(f.herdr, f.clock, nil)
	reconciler := NewReconciler(f.tg, f.herdr, testkit.NewMemMappingStore(), f.mapping, nil, f.clock, nil)
	b := NewBridge(cfg, f.herdr, f.tg, registry, reconciler, f.capture, nil, Services{Git: f.git, Inbox: f.inbox}, f.clock, nil)

	for range bridgeBuffer {
		b.Submit(AgentEvent{Kind: AgentChanged})
	}
	ctx, cancel := context.WithCancel(context.Background())
	accepted := make(chan error, 1)
	go func() { accepted <- b.SubmitContext(ctx, AgentEvent{Kind: AgentGone}) }()
	select {
	case err := <-accepted:
		t.Fatalf("full queue accepted extra state early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	// The buffered jobs drain once the loop runs, and cancelling stops it.
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(ctx)
	}()
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("state not accepted after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("state remained blocked after Run started")
	}
	waitUntil(t, "jobs drained", func() bool { return len(b.jobs) == 0 })
	if b.Dropped() != 0 {
		t.Fatalf("state dropped: %d", b.Dropped())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestBridgeServesButtonPress(t *testing.T) {
	r := newRunningBridge(t)
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusWorking)
	r.herdr.SetScreen("p1", dialogScreen)
	r.bridge.Submit(AgentEvent{Kind: AgentChanged, Agent: r.setStatus(a, domain.StatusBlocked)})
	waitUntil(t, "timer armed", func() bool { return r.bridge.out.deb.Pending() == 1 })
	r.clock.Advance(screenSettle)
	waitUntil(t, "question posted", func() bool { return len(r.tg.Sent()) == 1 })
	r.bridge.Submit(press(101, 1000, "3"))
	waitUntil(t, "press served", func() bool { return len(r.herdr.Keys()) == 1 })
	waitUntil(t, "answer sent", func() bool { return len(r.tg.Calls()) == 3 })
	assertCallsEqual(t, r.tg, r.tg.Calls()[0], "buttons:1000:✅ 3 · Синий", "answer:cb1:sent: 3")
}

func TestBridgeForgetsOnAgentGone(t *testing.T) {
	r := newRunningBridge(t)
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusWorking)
	r.herdr.SetScreen("p1", dialogScreen)
	r.bridge.Submit(AgentEvent{Kind: AgentChanged, Agent: r.setStatus(a, domain.StatusBlocked)})
	waitUntil(t, "timer armed", func() bool { return r.bridge.out.deb.Pending() == 1 })
	r.clock.Advance(screenSettle)
	waitUntil(t, "question posted", func() bool { return len(r.tg.Sent()) == 1 })
	gone := a
	gone.Status = domain.StatusExited
	r.bridge.Submit(AgentEvent{Kind: AgentGone, Agent: gone})
	waitUntil(t, "keyboard retired", func() bool { return len(r.tg.Calls()) == 2 })
	if calls := r.tg.Calls(); calls[1] != "buttons:1000:" {
		t.Fatalf("Calls = %q", calls)
	}
}

func TestBridgeRoutesClosePresses(t *testing.T) {
	r := newRunningBridge(t)
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusWorking)
	r.bridge.Submit(topicMsg(101, 5, "/close"))
	waitUntil(t, "close question", func() bool { return len(r.tg.Buttons(1000)) == 2 })
	r.bridge.Submit(domain.ButtonPressed{CallbackID: "cb1", ThreadID: 101, MessageID: 1000, FromID: 1, Data: "c:y"})
	waitUntil(t, "pane closed", func() bool { return len(r.herdr.Closed()) == 1 })
	waitUntil(t, "closing answer", func() bool {
		for _, c := range r.tg.Calls() {
			if c == "answer:cb1:closing" {
				return true
			}
		}
		return false
	})
	for _, c := range r.tg.Calls() {
		if strings.HasPrefix(c, "buttons:1000:") {
			t.Fatalf("close press reached outbound: %q", r.tg.Calls())
		}
	}
	// AgentGone drops a pending question on the inbound side.
	r.bridge.Submit(topicMsg(101, 6, "/close"))
	waitUntil(t, "second question", func() bool { return len(r.tg.Buttons(1001)) == 2 })
	delete(r.agents, a.Key)
	r.bridge.Submit(AgentEvent{Kind: AgentGone, Agent: a})
	waitUntil(t, "forget", func() bool {
		r.bridge.Submit(domain.ButtonPressed{CallbackID: "cb2", ThreadID: 101, MessageID: 1001, FromID: 1, Data: "c:y"})
		for _, c := range r.tg.Calls() {
			if c == "answer:cb2:not the latest question" {
				return true
			}
		}
		return false
	})
}

func TestBridgeRoutesPanelPressesToInbound(t *testing.T) {
	r := newRunningBridge(t)
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusBlocked)
	_ = a
	r.bridge.Submit(domain.ButtonPressed{CallbackID: "cb1", ThreadID: 0, MessageID: 900, FromID: 1, Data: dataHome()})
	waitUntil(t, "panel edit", func() bool {
		for _, c := range r.tg.Calls() {
			if strings.HasPrefix(c, "edittext:900:") {
				return true
			}
		}
		return false
	})
	// A digit press in a topic still goes to outbound: no keyboard is known
	// for the agent, so it is retired as stale.
	r.bridge.Submit(domain.ButtonPressed{CallbackID: "cb2", ThreadID: 101, MessageID: 55, FromID: 1, Data: "2"})
	waitUntil(t, "stale answer", func() bool {
		for _, c := range r.tg.Calls() {
			if strings.HasPrefix(c, "answer:cb2:") {
				return true
			}
		}
		return false
	})
	for _, c := range r.tg.Calls() {
		if strings.HasPrefix(c, "edittext:55") {
			t.Fatalf("digit press reached the panel: %q", r.tg.Calls())
		}
	}
}

func TestBridgeRunsCatchUpOnPresenceAway(t *testing.T) {
	r := newRunningBridge(t)
	quiet := true
	r.bridge.out.SetPresence(func() bool { return quiet }, r.opts)
	r.add(t, "p1", "t1", "reviewer", domain.StatusBlocked)
	r.herdr.SetScreen("p1", "\n  Allow edit?  \n  1. Yes  \n  2. No  \n\n")
	quiet = false
	r.bridge.Submit(presenceAway{})
	waitUntil(t, "catch-up post", func() bool { return len(r.tg.Sent()) == 1 })
	if sent := r.tg.Sent(); !sent[0].Notify || sent[0].ThreadID != 101 {
		t.Fatalf("catch-up post = %+v", sent)
	}
	r.bridge.Submit(struct{}{}) // unknown job types are still dropped
	if r.bridge.Dropped() != 0 {
		t.Error("unknown job counted as overflow")
	}
}

func TestBridgeNewRunsStartOffTheLoop(t *testing.T) {
	r := newRunningBridge(t)
	r.herdr.SetWorkspaces([]domain.Workspace{{ID: "w1", Label: "Work"}})
	r.herdr.SetStartDelay(300 * time.Millisecond)
	r.bridge.Submit(domain.GeneralCommand{MessageID: 1, FromID: 1, Text: "/new Work"})
	waitUntil(t, "starting reply", func() bool { return len(r.tg.Calls()) == 1 })
	// The start is still in flight; a /help must not wait behind it.
	r.bridge.Submit(domain.GeneralCommand{MessageID: 2, FromID: 1, Text: "/help"})
	waitUntil(t, "help reply", func() bool { return len(r.tg.Calls()) == 2 })
	if calls := r.tg.Calls(); !strings.HasPrefix(calls[1], "send:0:Commands") || len(r.herdr.Starts()) != 1 {
		t.Fatalf("calls = %q, starts = %d", calls, len(r.herdr.Starts()))
	}
	waitUntil(t, "started reply", func() bool { return len(r.tg.Calls()) == 3 })
	if calls := r.tg.Calls(); calls[2] != "send:0:started claude in Work (pane p-new):reply=1" {
		t.Fatalf("calls = %q", calls)
	}
}

func TestBridgeRunWaitsForSpawnedStarts(t *testing.T) {
	r := newRunningBridge(t)
	r.herdr.SetWorkspaces([]domain.Workspace{{ID: "w1", Label: "Work"}})
	r.herdr.SetStartDelay(time.Hour)
	r.bridge.Submit(domain.GeneralCommand{MessageID: 1, FromID: 1, Text: "/new Work"})
	waitUntil(t, "start in flight", func() bool { return len(r.herdr.Starts()) == 1 })
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel while a start was in flight")
	}
}

func TestBridgeTurnTimerFiresThroughRun(t *testing.T) {
	r := newRunningBridge(t)
	r.reactionsOn(t)
	a := r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.bridge.Submit(topicMsg(101, 4, "go"))
	waitUntil(t, "prompt acknowledgement", func() bool { return len(r.tg.Calls()) == 3 })
	r.bridge.Submit(AgentEvent{Kind: AgentChanged, Agent: r.setStatus(a, domain.StatusWorking)})
	r.bridge.Submit(AgentEvent{Kind: AgentChanged, Agent: r.setStatus(a, domain.StatusIdle)})
	waitUntil(t, "turn timer armed", func() bool { return r.clock.Pending() == 1 })
	r.clock.Advance(turnSettle)
	waitUntil(t, "idle completion", func() bool { return len(r.tg.Calls()) == 5 })
	if calls := r.tg.Calls(); len(calls) != 5 || calls[3] != "react:101:4:👌" || calls[4] != "send:101:screen" {
		t.Fatalf("turn completion calls = %q", calls)
	}
}

func TestBridgeRunsDownloadOffTheLoop(t *testing.T) {
	r := newRunningBridge(t)
	r.add(t, "p1", "t1", "reviewer", domain.StatusIdle)
	r.tg.SetFile("slow", []byte("x"))
	r.tg.SetDownloadDelay(300 * time.Millisecond)
	r.bridge.Submit(attachment(101, 1, domain.AttachmentPhoto, "slow", "", "pic", 1))
	r.bridge.Submit(topicMsg(101, 2, "meanwhile"))
	waitUntil(t, "prompt during download", func() bool { return len(r.herdr.Prompts()) == 1 })
	if p := r.herdr.Prompts(); p[0] != "p1: meanwhile" {
		t.Fatalf("first prompt = %q, the download blocked the loop", p[0])
	}
	waitUntil(t, "attachment prompt", func() bool { return len(r.herdr.Prompts()) == 2 })
	if p := r.herdr.Prompts(); !strings.HasPrefix(p[1], "p1: pic\n\n") {
		t.Fatalf("attachment prompt = %q", p[1])
	}
}

func TestPrivateFloodLeavesOwnerControlCapacity(t *testing.T) {
	busy := 0
	b := &Bridge{privateJobs: make(chan any, 32), control: make(chan any, 1), log: slog.New(slog.DiscardHandler), PrivateBusy: func(int64) { busy++ }}
	for range 100 {
		if err := b.SubmitContext(context.Background(), domain.PrivateMessage{Contact: domain.PrivateContact{ActorID: 10}}); err != nil {
			t.Fatal(err)
		}
	}
	if busy != 68 || len(b.privateJobs) != 32 {
		t.Fatal("private overload was not bounded and reported")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.SubmitContext(ctx, domain.GeneralCommand{Text: "/shares"}); err != nil {
		t.Fatal("private flood blocked owner command")
	}
	if len(b.control) != 1 {
		t.Fatal("owner command missing")
	}
}
