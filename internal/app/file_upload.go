package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

const telegramUploadLimit = 50_000_000

// UploadAgentFile sends a file only after resolving the pane against a fresh
// authoritative Herdr list and finding its exact currently-live topic.
func (d *Daemon) UploadAgentFile(ctx context.Context, request domain.AgentFileUpload) (string, error) {
	if request.PaneID == "" || (request.Kind != "photo" && request.Kind != "document") {
		return "", fmt.Errorf("invalid pane selector or upload kind")
	}
	if len(request.SHA256) != sha256.Size*2 {
		return "", fmt.Errorf("missing or invalid file snapshot digest")
	}
	path, err := filepath.Abs(filepath.Clean(request.Path))
	if err != nil {
		return "", fmt.Errorf("resolve file path: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("stat file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file")
	}
	if info.Size() <= 0 {
		return "", fmt.Errorf("file is empty")
	}
	if info.Size() > telegramUploadLimit {
		return "", fmt.Errorf("file exceeds 50 MB limit (%d bytes)", info.Size())
	}
	name := filepath.Base(request.Name)
	if name == "." || name == string(filepath.Separator) || strings.ContainsAny(name, "/\\\x00") {
		return "", fmt.Errorf("invalid file name")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat opened file: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return "", fmt.Errorf("file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, telegramUploadLimit+1))
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("file is empty")
	}
	if len(data) > telegramUploadLimit {
		return "", fmt.Errorf("file exceeds 50 MB limit")
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != strings.ToLower(request.SHA256) {
		return "", fmt.Errorf("file changed after CLI validation")
	}

	agent, err := d.registry.ResolvePane(ctx, request.PaneID)
	if err != nil {
		return "", err
	}
	threadID, ok := d.reconciler.ThreadForKey(agent.Key)
	if !ok {
		return "", fmt.Errorf("pane %q has no current live topic mapping", request.PaneID)
	}
	baseGuard := d.reconciler.DispatchGuard(agent.Key, threadID)
	guard := func(callCtx context.Context) (context.Context, context.CancelFunc, error) {
		currentAgent, err := d.registry.ResolvePane(callCtx, request.PaneID)
		if err != nil {
			return nil, nil, err
		}
		if currentAgent.Key != agent.Key {
			return nil, nil, fmt.Errorf("pane %q session changed before upload", request.PaneID)
		}
		currentThread, mapped := d.reconciler.ThreadForKey(currentAgent.Key)
		if !mapped || currentThread != threadID {
			return nil, nil, fmt.Errorf("pane %q topic mapping changed before upload", request.PaneID)
		}
		return baseGuard(callCtx)
	}
	if request.Kind == "photo" {
		if !strings.HasPrefix(http.DetectContentType(data), "image/") {
			return "", fmt.Errorf("file is not a recognized image")
		}
		err = d.tg.SendPhotoGuard(ctx, domain.Photo{ThreadID: threadID, Name: name, Data: data}, guard)
	} else {
		err = d.tg.SendOriginalDocument(ctx, domain.Document{ThreadID: threadID, Name: name, Data: data}, guard)
	}
	if err != nil {
		return "", fmt.Errorf("Telegram upload failed (no automatic retry): %w", err)
	}
	return fmt.Sprintf("sent %s to %s (pane %s)", name, agent.Name, request.PaneID), nil
}
