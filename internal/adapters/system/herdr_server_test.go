package system

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fakeHerdr(t *testing.T) (binary, calls string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake Herdr executable uses a POSIX shell")
	}
	dir := t.TempDir()
	parentPath := os.Getenv("PATH")
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	argsFile := filepath.Join(dir, "calls")
	readyFile := filepath.Join(dir, "ready")
	pidFile := filepath.Join(dir, "server.pid")
	herdrScript := `#!/bin/sh
printf '%s\n' "$*" >> "$HERDR_CALLS"
case "$3" in
status)
	if [ -f "$HERDR_READY" ]; then printf '{"running":true}\n'; else printf '{"running":false}\n'; fi
	;;
server)
	if [ "$4" = stop ]; then
		if [ -f "$HERDR_PID" ]; then server_pid=$(cat "$HERDR_PID"); kill -KILL -- "-$server_pid" 2>/dev/null || true; fi
		rm -f "$HERDR_READY"
		if [ "$HERDR_MODE" = descendant ]; then ` + sleepPath + ` 0.2 & fi
		if [ "$HERDR_MODE" = stop-fail ]; then printf 'server unavailable' >&2; exit 17; fi
		exit 0
	fi
	echo $$ > "$HERDR_PID"
	case "$HERDR_MODE" in
	delayed) (` + sleepPath + ` 0.12; touch "$HERDR_READY") & ;;
	never) ;;
	exit) printf 'invalid server configuration' >&2; exit 17 ;;
	ready) touch "$HERDR_READY" ;;
	esac
	` + sleepPath + ` 30
	;;
esac
`
	herdrPath := filepath.Join(dir, "herdr")
	if err := os.WriteFile(herdrPath, []byte(herdrScript), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+parentPath)
	t.Setenv("HERDR_CALLS", argsFile)
	t.Setenv("HERDR_READY", readyFile)
	t.Setenv("HERDR_PID", pidFile)
	t.Setenv("HERDR_MODE", "")
	t.Cleanup(func() {
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
				terminateProcessGroup(pid)
			}
		}
	})
	return herdrPath, argsFile
}

func quickHerdrTiming(t *testing.T) {
	t.Helper()
	oldReady, oldPoll, oldCommand, oldWait := herdrServerReadyTimeout, herdrServerPollInterval, herdrCommandTimeout, herdrCommandWaitDelay
	herdrServerReadyTimeout = 500 * time.Millisecond
	herdrServerPollInterval = 15 * time.Millisecond
	herdrCommandTimeout = 100 * time.Millisecond
	herdrCommandWaitDelay = 50 * time.Millisecond
	t.Cleanup(func() {
		herdrServerReadyTimeout, herdrServerPollInterval, herdrCommandTimeout, herdrCommandWaitDelay = oldReady, oldPoll, oldCommand, oldWait
	})
}

func TestControlHerdrServerUsesNamedDefaultAndWaitsForReady(t *testing.T) {
	quickHerdrTiming(t)
	binary, calls := fakeHerdr(t)
	t.Setenv("HERDR_MODE", "delayed")
	started := time.Now()
	startCtx, cancelStart := context.WithCancel(context.Background())
	if err := ControlHerdrServer(startCtx, binary, false); err != nil {
		t.Fatal(err)
	}
	cancelStart()
	status, err := readHerdrServerStatus(context.Background(), binary)
	if err != nil || !status.Running {
		t.Fatalf("detached server did not survive request context cancellation: status=%+v err=%v", status, err)
	}
	if time.Since(started) < 100*time.Millisecond {
		t.Fatal("start returned before delayed default-session readiness")
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "--session default status server --json") || !strings.Contains(text, "--session default server\n") {
		t.Fatalf("Herdr calls did not target default session or check readiness: %q", text)
	}
	if err := ControlHerdrServer(context.Background(), binary, true); err != nil {
		t.Fatalf("stop default test server: %v", err)
	}
}

func TestControlHerdrServerAlreadyRunningDoesNotLaunchAnother(t *testing.T) {
	quickHerdrTiming(t)
	binary, calls := fakeHerdr(t)
	if err := os.WriteFile(os.Getenv("HERDR_READY"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ControlHerdrServer(context.Background(), binary, false); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "--session default status server --json\n" {
		t.Fatalf("already-running start spawned a second server: %q", got)
	}
}

func TestControlHerdrServerNoReadinessIsBoundedAndKillsProcessGroup(t *testing.T) {
	quickHerdrTiming(t)
	binary, calls := fakeHerdr(t)
	t.Setenv("HERDR_MODE", "never")
	started := time.Now()
	err := ControlHerdrServer(context.Background(), binary, false)
	if err == nil || !strings.Contains(err.Error(), "did not report running") {
		t.Fatalf("start error = %v, want a readiness timeout", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("unready start took too long: %s", time.Since(started))
	}
	data, readErr := os.ReadFile(calls)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(data), "--session default server") {
		t.Fatalf("no headless start command was run: %q", data)
	}
}

func TestControlHerdrServerReportsEarlyExit(t *testing.T) {
	quickHerdrTiming(t)
	binary, _ := fakeHerdr(t)
	t.Setenv("HERDR_MODE", "exit")
	err := ControlHerdrServer(context.Background(), binary, false)
	if err == nil || !strings.Contains(err.Error(), "exited before its default-session socket became ready") {
		t.Fatalf("early server exit = %v", err)
	}
}

func TestControlHerdrServerStopWithDescendantStdoutIsBounded(t *testing.T) {
	quickHerdrTiming(t)
	binary, _ := fakeHerdr(t)
	t.Setenv("HERDR_MODE", "descendant")
	started := time.Now()
	if err := ControlHerdrServer(context.Background(), binary, true); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("stop waited for descendant-held output: %s", time.Since(started))
	}
}

func TestControlHerdrServerStopReportsFailure(t *testing.T) {
	quickHerdrTiming(t)
	binary, _ := fakeHerdr(t)
	t.Setenv("HERDR_MODE", "stop-fail")
	err := ControlHerdrServer(context.Background(), binary, true)
	if err == nil || !strings.Contains(err.Error(), "server unavailable") {
		t.Fatalf("stop failure = %v", err)
	}
}

func TestControlHerdrServerUsesInjectedAbsoluteBinaryWithoutPATHLookup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake executable uses a POSIX shell")
	}
	dir := t.TempDir()
	pathlessDir := filepath.Join(dir, "empty-path")
	if err := os.Mkdir(pathlessDir, 0o700); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(dir, "calls")
	binary := filepath.Join(dir, "configured-herdr")
	script := `#!/bin/sh
printf '%s %s\n' "$3" "$4" >> "$HERDR_CALLS"
if [ "$3" = status ]; then printf '{"running":true}\n'; fi
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathlessDir)
	t.Setenv("HERDR_CALLS", calls)
	if err := ControlHerdrServer(context.Background(), binary, false); err != nil {
		t.Fatalf("start using injected path: %v", err)
	}
	if err := ControlHerdrServer(context.Background(), binary, true); err != nil {
		t.Fatalf("stop using injected path: %v", err)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "status server\nserver stop\n"; got != want {
		t.Fatalf("invocations = %q, want %q", got, want)
	}
}
