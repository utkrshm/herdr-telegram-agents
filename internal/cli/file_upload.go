package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

const (
	imagePhotoMax = 10_000_000
	fileUploadMax = 50_000_000
)

func runSendImage(rc *runContext, args []string) int { return runFileUpload(rc, args, "photo") }
func runSendFile(rc *runContext, args []string) int  { return runFileUpload(rc, args, "document") }

func runFileUpload(rc *runContext, args []string, kind string) int {
	paneID := strings.TrimSpace(os.Getenv("HERDR_PANE_ID"))
	if len(args) >= 2 && args[0] == "--pane" {
		paneID, args = strings.TrimSpace(args[1]), args[2:]
	}
	if paneID == "" || len(args) == 0 {
		fmt.Fprintf(rc.stderr, "usage: herdr-tg %s [--pane <id>] <file>... (HERDR_PANE_ID is required unless --pane is set)\n", kindCommand(kind))
		return exitUsage
	}
	env, err := wire.agentCommandEnv()
	if err != nil {
		fmt.Fprintf(rc.stderr, "herdr-tg %s: %v\n", kindCommand(kind), err)
		return exitError
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := 0
	for _, inputPath := range args {
		absolute, err := filepath.Abs(inputPath)
		if err != nil {
			reportFileResult(rc, inputPath, err)
			failed++
			continue
		}
		info, err := os.Lstat(absolute)
		if err != nil {
			reportFileResult(rc, inputPath, err)
			failed++
			continue
		}
		if !info.Mode().IsRegular() {
			reportFileResult(rc, inputPath, fmt.Errorf("not a regular file"))
			failed++
			continue
		}
		if info.Size() > fileUploadMax {
			reportFileResult(rc, inputPath, fmt.Errorf("exceeds 50 MB limit (%d bytes)", info.Size()))
			failed++
			continue
		}
		if info.Size() == 0 {
			reportFileResult(rc, inputPath, fmt.Errorf("file is empty"))
			failed++
			continue
		}
		digest, err := fileSnapshotDigest(absolute, info)
		if err != nil {
			reportFileResult(rc, inputPath, err)
			failed++
			continue
		}
		deliveryKind := kind
		if kind == "photo" && info.Size() > imagePhotoMax {
			deliveryKind = "document"
		}
		request := domain.AgentFileUpload{PaneID: paneID, Path: absolute, Name: filepath.Base(absolute), Kind: deliveryKind, SHA256: digest}
		result, err := wire.sendAgentFile(ctx, env, request)
		if err != nil {
			reportFileResult(rc, inputPath, err)
			failed++
			continue
		}
		if deliveryKind != kind {
			result += " (over 10 MB; sent as document)"
		}
		fmt.Fprintf(rc.stdout, "%s: %s\n", inputPath, result)
	}
	if failed > 0 {
		return exitError
	}
	return exitOK
}

func fileSnapshotDigest(path string, selected os.FileInfo) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat opened file: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(selected, opened) {
		return "", fmt.Errorf("file changed before validation")
	}
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, fileUploadMax+1))
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}
	if count != selected.Size() || count > fileUploadMax {
		return "", fmt.Errorf("file changed or exceeds 50 MB limit")
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(selected, current) {
		return "", fmt.Errorf("file changed during validation")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func kindCommand(kind string) string {
	if kind == "photo" {
		return "send-image"
	}
	return "send-file"
}

func reportFileResult(rc *runContext, name string, err error) {
	fmt.Fprintf(rc.stdout, "%s: failed: %v\n", name, err)
}
