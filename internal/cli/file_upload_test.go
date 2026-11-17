package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/compose"
	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestSendImageMultipleFilesUsesPhotoAndDocumentFallback(t *testing.T) {
	first := filepath.Join(t.TempDir(), "ordinary.png")
	second := filepath.Join(t.TempDir(), "large.png")
	if err := os.WriteFile(first, []byte("photo"), 0o600); err != nil {
		t.Fatal(err)
	}
	large, err := os.Create(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := large.Truncate(imagePhotoMax + 1); err != nil {
		t.Fatal(err)
	}
	_ = large.Close()
	oldWire := wire
	t.Cleanup(func() { wire = oldWire })
	wire.agentCommandEnv = func() (compose.PluginEnv, error) { return compose.PluginEnv{StateDir: "/state"}, nil }
	var requests []domain.AgentFileUpload
	wire.sendAgentFile = func(_ context.Context, _ compose.PluginEnv, request domain.AgentFileUpload) (string, error) {
		requests = append(requests, request)
		return "sent " + request.Name, nil
	}
	var out, errOut bytes.Buffer
	rc := &runContext{stdout: &out, stderr: &errOut, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if code := runSendImage(rc, []string{"--pane", "pane/A", first, second}); code != exitOK {
		t.Fatalf("exit=%d stderr=%s", code, errOut.String())
	}
	largeData, _ := os.ReadFile(second)
	largeHash := sha256.Sum256(largeData)
	firstHash := sha256.Sum256([]byte("photo"))
	want := []domain.AgentFileUpload{
		{PaneID: "pane/A", Path: first, Name: "ordinary.png", Kind: "photo", SHA256: hex.EncodeToString(firstHash[:])},
		{PaneID: "pane/A", Path: second, Name: "large.png", Kind: "document", SHA256: hex.EncodeToString(largeHash[:])},
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests=%+v want %+v", requests, want)
	}
	if !strings.Contains(out.String(), "sent as document") {
		t.Fatalf("output=%s", out.String())
	}
}

func TestSendFileMissingAndOversizedFilesReportPerFileFailures(t *testing.T) {
	large := filepath.Join(t.TempDir(), "large.json")
	f, err := os.Create(large)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(fileUploadMax + 1); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	oldWire := wire
	t.Cleanup(func() { wire = oldWire })
	wire.agentCommandEnv = func() (compose.PluginEnv, error) { return compose.PluginEnv{StateDir: "/state"}, nil }
	wire.sendAgentFile = func(context.Context, compose.PluginEnv, domain.AgentFileUpload) (string, error) {
		t.Fatal("invalid file reached daemon")
		return "", nil
	}
	var out, errOut bytes.Buffer
	rc := &runContext{stdout: &out, stderr: &errOut, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	code := runSendFile(rc, []string{"--pane", "p", filepath.Join(t.TempDir(), "missing"), large})
	if code != exitError {
		t.Fatalf("exit=%d", code)
	}
	if !strings.Contains(out.String(), "no such file") || !strings.Contains(out.String(), "50 MB limit") {
		t.Fatalf("output=%s", out.String())
	}
}

func TestSendCommandsRequirePaneSelector(t *testing.T) {
	old := os.Getenv("HERDR_PANE_ID")
	_ = os.Unsetenv("HERDR_PANE_ID")
	t.Cleanup(func() { _ = os.Setenv("HERDR_PANE_ID", old) })
	var stderr bytes.Buffer
	rc := &runContext{stderr: &stderr}
	if code := runSendFile(rc, []string{"file.txt"}); code != exitUsage {
		t.Fatalf("exit=%d", code)
	}
	if !strings.Contains(stderr.String(), "HERDR_PANE_ID is required") {
		t.Fatalf("usage=%s", stderr.String())
	}
}
