package app_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestAgentFileUploadRoutesByFreshPaneAndRejectsStalePane(t *testing.T) {
	f := newDaemon(t)
	first, second := agent("pane-one", "term-one", "one", domain.StatusIdle), agent("pane-two", "term-two", "two", domain.StatusIdle)
	f.herdr.SetAgents([]domain.Agent{first, second})
	f.start(t)
	waitFor(t, "both mapped topics", func() bool { return countCalls(f.tg.Calls(), "create:") == 2 })
	file := filepath.Join(t.TempDir(), "diagram.png")
	if err := os.WriteFile(file, []byte("\x89PNG\r\n\x1a\nimage"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("\x89PNG\r\n\x1a\nimage"))
	request := domain.AgentFileUpload{PaneID: "pane-two", Path: file, Name: "diagram.png", Kind: "photo", SHA256: hex.EncodeToString(digest[:])}
	result, err := f.daemon.UploadAgentFile(context.Background(), request)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if result == "" {
		t.Fatal("upload result is empty")
	}
	photos := f.tg.Photos()
	if len(photos) != 1 || photos[0].ThreadID == 0 || string(photos[0].Data) != "\x89PNG\r\n\x1a\nimage" {
		t.Fatalf("photos=%+v", photos)
	}
	secondThread, ok := f.rec.ThreadForKey(second.Key)
	if !ok || photos[0].ThreadID != secondThread {
		t.Fatalf("photo thread=%d, pane-two thread=%d", photos[0].ThreadID, secondThread)
	}
	guard := f.rec.DispatchGuard(second.Key, secondThread)
	if err := f.rec.OnTopicClosed(context.Background(), secondThread); err != nil {
		t.Fatalf("close topic: %v", err)
	}
	if _, _, err := guard(context.Background()); err == nil {
		t.Fatal("dispatch guard accepted a topic closed while queued")
	}
	beforeClosedUpload := len(f.tg.Photos())
	if _, err := f.daemon.UploadAgentFile(context.Background(), request); err == nil {
		t.Fatal("closed topic accepted an upload")
	}
	if len(f.tg.Photos()) != beforeClosedUpload {
		t.Fatal("closed topic upload reached Telegram")
	}

	f.herdr.SetAgents([]domain.Agent{first})
	before := len(f.tg.Photos())
	if _, err := f.daemon.UploadAgentFile(context.Background(), request); err == nil {
		t.Fatal("stale pane was accepted")
	}
	if len(f.tg.Photos()) != before {
		t.Fatal("stale pane caused a Telegram upload")
	}
}

func TestAgentFileUploadRejectsSubstitutedPathSnapshot(t *testing.T) {
	f := newDaemon(t)
	first := agent("pane-one", "term-one", "one", domain.StatusIdle)
	f.herdr.SetAgents([]domain.Agent{first})
	f.start(t)
	waitFor(t, "mapped topic", func() bool { return countCalls(f.tg.Calls(), "create:") == 1 })
	path := filepath.Join(t.TempDir(), "file.json")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("original"))
	if err := os.WriteFile(path, []byte("substituted"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := f.daemon.UploadAgentFile(context.Background(), domain.AgentFileUpload{PaneID: first.Key.PaneID, Path: path, Name: "file.json", Kind: "document", SHA256: hex.EncodeToString(digest[:])})
	if err == nil || !strings.Contains(err.Error(), "changed after CLI validation") {
		t.Fatalf("err=%v", err)
	}
	if len(f.tg.Documents()) != 0 {
		t.Fatal("substituted file reached Telegram")
	}
}
