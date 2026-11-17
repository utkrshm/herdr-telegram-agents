package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

type queuedReplyGateway struct {
	domain.TelegramGateway
	started chan string
	release chan error
}

func (g *queuedReplyGateway) SendGuard(ctx context.Context, out domain.Outgoing, guard domain.DispatchGuard) (int, error) {
	call, cancel, err := guard(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	g.started <- out.Text
	select {
	case err := <-g.release:
		return 23, err
	case <-call.Done():
		return 0, call.Err()
	}
}

func setupQueuedBridge(t *testing.T) (*runningBridge, *queuedReplyGateway, domain.Agent, domain.Agent) {
	t.Helper()
	var g *queuedReplyGateway
	var a, b domain.Agent
	r := newRunningBridge(t, func(f *bridgeFixture, bridge *Bridge) {
		a = f.add(t, "p1", "t1", "one", domain.StatusIdle)
		b = f.add(t, "p2", "t2", "two", domain.StatusIdle)
		f.herdr.SetScreen("p1", "first screen")
		f.herdr.SetScreen("p2", "second screen")
		g = &queuedReplyGateway{TelegramGateway: f.tg, started: make(chan string, 5), release: make(chan error, 5)}
		bridge.out.tg = newRedactingGateway(g, nil, nil, nil)
	})
	return r, g, a, b
}

func recvText(t *testing.T, c <-chan string) string {
	t.Helper()
	select {
	case s := <-c:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for send")
		return ""
	}
}

func TestBridgeScreenAsyncFailureDoesNotBlockOtherReply(t *testing.T) {
	r, g, _, _ := setupQueuedBridge(t)
	r.bridge.Submit(topicMsg(101, 1, "/screen 10"))
	if s := recvText(t, g.started); s != "first screen" {
		t.Fatal(s)
	}
	r.bridge.Submit(topicMsg(102, 2, "/screen 10"))
	r.bridge.Submit(domain.GeneralCommand{MessageID: 3, FromID: 1, Text: "/help"})
	waitUntil(t, "help while screen is blocked", func() bool {
		for _, out := range r.tg.Sent() {
			if out.Text == helpText {
				return true
			}
		}
		return false
	})
	g.release <- errors.New("part 2 failed")
	if s := recvText(t, g.started); s != "second screen" {
		t.Fatal(s)
	}
	g.release <- nil
	waitUntil(t, "failure notice", func() bool {
		for _, out := range r.tg.Sent() {
			if strings.Contains(out.Text, "Delivery to topic 101 stopped") {
				return true
			}
		}
		return false
	})
}

func TestBridgeScreenQueuedMappingChangeIsQuarantined(t *testing.T) {
	r, g, a, b := setupQueuedBridge(t)
	r.bridge.Submit(topicMsg(101, 1, "/screen 10"))
	_ = recvText(t, g.started)
	r.bridge.Submit(topicMsg(102, 2, "/screen 10"))
	waitUntil(t, "second screen queued", func() bool { return len(r.bridge.ownerReplies) == 1 })
	m := domain.NewMapping(-1001234567890)
	m.Link(a.Key, domain.Topic{ThreadID: 101}, a, tb0)
	other := b
	other.Key.PaneID = "replacement"
	m.Link(other.Key, domain.Topic{ThreadID: 102}, other, tb0)
	r.view.publish(m)
	g.release <- nil
	waitUntil(t, "stale notice", func() bool {
		for _, out := range r.tg.Sent() {
			if strings.Contains(out.Text, "Delivery to topic 102 stopped") {
				return true
			}
		}
		return false
	})
	select {
	case s := <-g.started:
		t.Fatalf("stale screen sent: %s", s)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestBridgeInvalidatedFullQueueReleasesAdmission(t *testing.T) {
	r, g, a, b := setupQueuedBridge(t)
	r.bridge.Submit(topicMsg(101, 1, "/screen 10"))
	if got := recvText(t, g.started); got != "first screen" {
		t.Fatal(got)
	}
	for n := 1; n < ownerReplyCapacity; n++ {
		r.bridge.Submit(topicMsg(102, n+1, "/screen 10"))
	}
	waitUntil(t, "full owner reply admission", func() bool { return len(r.bridge.ownerReplySlots) == ownerReplyCapacity })
	// Cancel both the in-flight request and every queued generation.
	r.view.publish(domain.NewMapping(-1001234567890))
	waitUntil(t, "all invalidated replies completed", func() bool { return len(r.bridge.ownerReplySlots) == 0 })

	m := domain.NewMapping(-1001234567890)
	m.Link(a.Key, domain.Topic{ThreadID: 101}, a, tb0)
	m.Link(b.Key, domain.Topic{ThreadID: 102}, b, tb0)
	r.view.publish(m)
	r.bridge.Submit(topicMsg(101, 100, "/screen 10"))
	if got := recvText(t, g.started); got != "first screen" {
		t.Fatalf("fresh request = %q", got)
	}
	g.release <- nil
	waitUntil(t, "fresh request completed", func() bool { return len(r.bridge.ownerReplySlots) == 0 })
	select {
	case got := <-g.started:
		t.Fatalf("invalidated reply was dispatched: %q", got)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestOwnerReplyQuarantineEvictsAndReports(t *testing.T) {
	f := newBridgeFixture(t)
	b := &Bridge{out: f.out, log: f.out.log, ownerNoticeSlot: make(chan struct{}, 1)}
	for n := 0; n <= ownerQuarantineCapacity; n++ {
		b.quarantineOwnerReply(ownerReplyResult{
			reply:  ownerReply{out: domain.Outgoing{ThreadID: n + 100, Text: "failed source"}},
			lastID: n, err: errors.New("partial delivery"),
		})
	}
	b.wg.Wait()
	if len(b.failedOwnerReplies) != ownerQuarantineCapacity {
		t.Fatalf("quarantine length = %d", len(b.failedOwnerReplies))
	}
	if b.failedOwnerReplies[0].lastID != 1 || b.failedOwnerReplies[ownerQuarantineCapacity-1].lastID != ownerQuarantineCapacity {
		t.Fatal("quarantine did not retain the newest failures")
	}
	if !strings.Contains(f.logBuf.String(), "quarantine evicted") {
		t.Fatal("eviction not logged")
	}
	sent := f.tg.Sent()
	if len(sent) != 1 || !strings.Contains(sent[0].Text, "evicted from memory") || !strings.Contains(sent[0].Text, "not retried") {
		t.Fatalf("eviction notice = %+v", sent)
	}
}

func TestBridgeDoneReplyUsesAsyncQueue(t *testing.T) {
	var g *queuedReplyGateway
	var agent domain.Agent
	r := newRunningBridge(t, func(f *bridgeFixture, b *Bridge) {
		agent = f.add(t, "p1", "t1", "one", domain.StatusDone)
		f.replies.Set(agent.Key, strings.Repeat("Sentence. ", 1000))
		b.out.replies = f.replies
		b.out.doneMode = func() domain.DoneMode { return domain.DoneFormatted }
		g = &queuedReplyGateway{TelegramGateway: f.tg, started: make(chan string, 2), release: make(chan error, 2)}
		b.out.tg = newRedactingGateway(g, nil, nil, nil)
	})
	r.bridge.Submit(AgentEvent{Kind: AgentChanged, Agent: agent})
	waitUntil(t, "done timer", func() bool { return r.clock.Pending() > 0 })
	r.clock.Advance(screenSettle)
	if got := recvText(t, g.started); !strings.HasPrefix(got, "Sentence.") {
		t.Fatal("missing reply")
	}
	r.bridge.Submit(domain.GeneralCommand{MessageID: 5, FromID: 1, Text: "/help"})
	waitUntil(t, "help while done reply blocked", func() bool {
		for _, out := range r.tg.Sent() {
			if out.Text == helpText {
				return true
			}
		}
		return false
	})
	g.release <- nil
}

func TestBridgeBareScreenTranscriptUsesAsyncQueue(t *testing.T) {
	var g *queuedReplyGateway
	r := newRunningBridge(t, func(f *bridgeFixture, b *Bridge) {
		a := f.add(t, "p1", "t1", "one", domain.StatusIdle)
		a.Kind = "opencode"
		f.agents[a.Key] = a
		f.replies.Set(a.Key, strings.Repeat("🌍 done. ", 1000))
		b.out.replies = f.replies
		g = &queuedReplyGateway{TelegramGateway: f.tg, started: make(chan string, 2), release: make(chan error, 2)}
		b.out.tg = newRedactingGateway(g, nil, nil, nil)
	})
	r.bridge.Submit(topicMsg(101, 1, "/screen"))
	if got := recvText(t, g.started); !strings.HasPrefix(got, "🌍 done.") {
		t.Fatalf("screen reply = %q", got)
	}
	r.bridge.Submit(domain.GeneralCommand{MessageID: 5, FromID: 1, Text: "/help"})
	waitUntil(t, "help while screen reply blocked", func() bool {
		for _, out := range r.tg.Sent() {
			if out.Text == helpText {
				return true
			}
		}
		return false
	})
	g.release <- nil
}

func TestOwnerReplyResultDoesNotOverwriteNewerPost(t *testing.T) {
	f := newBridgeFixture(t)
	a := f.add(t, "p1", "t1", "one", domain.StatusIdle)
	gen, err := f.view.generation(a.Key, 101)
	if err != nil {
		t.Fatal(err)
	}
	f.out.queuedReplies[a.Key] = "old"
	f.out.replyRevision[a.Key] = 2
	f.out.lastPosted[a.Key] = "new"
	b := &Bridge{out: f.out, ownerReplySlots: make(chan struct{}, 1)}
	b.ownerReplySlots <- struct{}{}
	b.handle(context.Background(), ownerReplyResult{reply: ownerReply{key: a.Key, hash: "old", revision: 1, generation: gen}})
	if got := f.out.lastPosted[a.Key]; got != "new" {
		t.Fatalf("last posted overwritten: %s", got)
	}

	f.out.replyRevision[a.Key] = 3
	f.out.queuedReplies[a.Key] = "stale"
	gen.cancel()
	b.ownerReplySlots <- struct{}{}
	b.handle(context.Background(), ownerReplyResult{reply: ownerReply{key: a.Key, hash: "stale", revision: 3, generation: gen}})
	if got := f.out.lastPosted[a.Key]; got != "new" {
		t.Fatalf("stale result overwrote last posted: %s", got)
	}
}
