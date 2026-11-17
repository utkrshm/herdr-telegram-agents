package app

import (
	"context"
	"strings"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// Constants exposed to the external test package.
const SweepIntervalForTest = sweepInterval

// The private mirror's wait for a pending reply.
const (
	ReplyPendingDelayForTest   = replyPendingDelay
	ReplyPendingRetriesForTest = replyPendingRetries
)

func TestExportRoutingAndDocument(t *testing.T) {
	f := newBridgeFixture(t)
	a := f.add(t, "p", "t", "a", domain.StatusIdle)
	delete(f.agents, a.Key)
	entry := f.mapping.Topics[a.Key.String()]
	delete(f.mapping.Topics, a.Key.String())
	a.Kind = "opencode"
	a.SessionDigest = (domain.SessionTuple{Source: "herdr:opencode", Agent: "opencode", Kind: "id", Value: "ses"}).Digest()
	f.agents[a.Key] = a
	f.mapping.Topics[a.Key.String()] = entry
	f.view.publish(f.mapping)
	f.in.exportTranscript = func(_ context.Context, got domain.Agent) ([]byte, error) {
		if got.Key != a.Key {
			t.Fatalf("wrong agent: %+v", got)
		}
		return []byte("hello\n" + testBotToken), nil
	}
	if err := f.in.HandleTopic(f.ctx, topicMsg(101, 8, "/export@agents_bot")); err != nil {
		t.Fatal(err)
	}
	docs := f.tg.Documents()
	if len(docs) != 1 || docs[0].ThreadID != 101 || docs[0].ReplyTo != 8 || docs[0].Name != "opencode-transcript.md" || !strings.Contains(string(docs[0].Data), "hello\n") || strings.Contains(string(docs[0].Data), testBotToken) {
		t.Fatalf("documents: %+v", docs)
	}
	if len(f.herdr.Prompts()) != 0 {
		t.Fatal("export forwarded as prompt")
	}
}

func TestExportRefusesUnsupportedMissingAndGeneral(t *testing.T) {
	f := newBridgeFixture(t)
	a := f.add(t, "p", "t", "a", domain.StatusIdle)
	f.withKind(a, "claude")
	if err := f.in.HandleTopic(f.ctx, topicMsg(101, 1, "/export")); err != nil {
		t.Fatal(err)
	}
	f.withKind(a, "opencode")
	if err := f.in.HandleTopic(f.ctx, topicMsg(101, 2, "/export")); err != nil {
		t.Fatal(err)
	}
	if err := f.in.HandleGeneral(f.ctx, domain.GeneralCommand{MessageID: 3, Text: "/export"}); err != nil {
		t.Fatal(err)
	}
	if len(f.tg.Documents()) != 0 || len(f.tg.Sent()) != 3 {
		t.Fatalf("documents=%v replies=%v", f.tg.Documents(), f.tg.Sent())
	}
	if !strings.Contains(f.tg.Sent()[0].Text, "only for OpenCode") || !strings.Contains(f.tg.Sent()[1].Text, "no verified") || f.tg.Sent()[2].Text != topicOnly {
		t.Fatalf("replies=%v", f.tg.Sent())
	}
}

func TestExportRejectsOversizeAndRemapping(t *testing.T) {
	for _, remap := range []bool{false, true} {
		f := newBridgeFixture(t)
		a := f.add(t, "p", "t", "a", domain.StatusIdle)
		delete(f.agents, a.Key)
		entry := f.mapping.Topics[a.Key.String()]
		delete(f.mapping.Topics, a.Key.String())
		a.Kind = "opencode"
		a.SessionDigest = (domain.SessionTuple{Source: "s", Agent: "opencode", Kind: "id", Value: "v"}).Digest()
		f.agents[a.Key] = a
		f.mapping.Topics[a.Key.String()] = entry
		f.view.publish(f.mapping)
		f.in.exportTranscript = func(context.Context, domain.Agent) ([]byte, error) {
			if remap {
				delete(f.mapping.Topics, a.Key.String())
				f.view.publish(f.mapping)
				return []byte("private"), nil
			}
			return make([]byte, telegramDocumentLimit+1), nil
		}
		if err := f.in.HandleTopic(f.ctx, topicMsg(101, 5, "/export")); err != nil {
			t.Fatal(err)
		}
		if len(f.tg.Documents()) != 0 {
			t.Fatal("unexpected document")
		}
		if !remap && (len(f.tg.Sent()) != 1 || !strings.Contains(f.tg.Sent()[0].Text, "50 MB")) {
			t.Fatalf("missing limit notice: %v", f.tg.Sent())
		}
	}
}
