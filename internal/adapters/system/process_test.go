//go:build !windows

package system

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
	"github.com/permgps/herdr-telegram-agents/internal/testkit"
)

func waitUntil(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestAliveOwnAndReapedPid(t *testing.T) {
	p := NewProcess(t.TempDir(), nil)
	if !p.Alive(os.Getpid()) {
		t.Fatal("own pid reported dead")
	}
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skip("no `true` binary:", err)
	}
	if p.Alive(cmd.Process.Pid) {
		t.Fatal("reaped child reported alive")
	}
	if p.Alive(0) || p.Alive(-1) {
		t.Fatal("non-positive pid reported alive")
	}
}

func TestSpawnStopAndKill(t *testing.T) {
	dir := t.TempDir()
	p := NewProcess(dir, nil)
	p.exe = "/bin/sh"

	pid, err := p.Spawn(context.Background(), []string{"-c", "echo spawned; sleep 30"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(pid) })
	waitUntil(t, func() bool { return p.Alive(pid) }, "child alive")
	waitUntil(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(dir, ErrLogFileName))
		return string(b) == "spawned\n"
	}, "child output in err log")

	if err := p.Resync(pid, true); err != nil {
		// sh ignores nothing by default: SIGHUP ends it, which is fine here.
		t.Fatalf("Resync = %v", err)
	}
	waitUntil(t, func() bool { return !p.Alive(pid) }, "child gone after SIGHUP")

	pid, err = p.Spawn(context.Background(), []string{"-c", "trap '' HUP; sleep 30"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(pid) })
	waitUntil(t, func() bool { return p.Alive(pid) }, "second child alive")
	if err := p.Stop(pid, true); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	waitUntil(t, func() bool { return !p.Alive(pid) }, "child gone after SIGTERM")
	if err := p.Stop(pid, true); !errors.Is(err, domain.ErrNotRunning) {
		t.Fatalf("Stop on dead pid = %v, want ErrNotRunning", err)
	}
}

func TestSpawnSurvivesContextCancel(t *testing.T) {
	p := NewProcess(t.TempDir(), nil)
	p.exe = "/bin/sh"
	ctx, cancel := context.WithCancel(context.Background())
	pid, err := p.Spawn(ctx, []string{"-c", "sleep 30"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(pid) })
	cancel()
	time.Sleep(100 * time.Millisecond)
	if !p.Alive(pid) {
		t.Fatal("child died when the spawning context was cancelled")
	}
	if err := p.Kill(pid); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return !p.Alive(pid) }, "child gone after SIGKILL")
}

func TestPluginRestartSpawnerSubprocess(t *testing.T) {
	if os.Getenv("HERDR_TG_RESTART_SPAWNER") != "1" {
		return
	}
	_, _, err := launchPluginRestart(context.Background(), os.Getenv("HERDR_TG_RESTART_STATE"), os.Getenv("HERDR_TG_RESTART_SCRIPT"), nil)
	if err != nil {
		os.Exit(21)
	}
	// Simulate the daemon exiting immediately after launching the detached
	// supervisor action. The outer test verifies the action still completed.
	os.Exit(0)
}

func TestPluginRestartHelperArgsEnvironmentAndParentLifetime(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "helper")
	script := filepath.Join(dir, "restart-helper.sh")
	body := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$HELPER_OUTPUT.args\"\nprintf '%s|%s\\n' \"$HERDR_PLUGIN_ROOT\" \"$HELPER_MARKER\" > \"$HELPER_OUTPUT.env\"\nsleep 0.25\nprintf done > \"$HELPER_OUTPUT.done\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestPluginRestartSpawnerSubprocess$")
	cmd.Env = append(os.Environ(),
		"HERDR_TG_RESTART_SPAWNER=1",
		"HERDR_TG_RESTART_STATE="+filepath.Join(dir, "state"),
		"HERDR_TG_RESTART_SCRIPT="+script,
		"HELPER_OUTPUT="+output,
		"HERDR_PLUGIN_ROOT=/isolated/plugin/root",
		"HELPER_MARKER=inherited-from-daemon",
	)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("spawner subprocess failed: %v\n%s", err, combined)
	}
	waitUntil(t, func() bool {
		_, err := os.Stat(output + ".done")
		return err == nil
	}, "detached restart helper after launcher exits")

	args, err := os.ReadFile(output + ".args")
	if err != nil {
		t.Fatal(err)
	}
	if string(args) != "action\nrestart\n" {
		t.Fatalf("helper args = %q, want action restart", args)
	}
	env, err := os.ReadFile(output + ".env")
	if err != nil {
		t.Fatal(err)
	}
	if string(env) != "/isolated/plugin/root|inherited-from-daemon\n" {
		t.Fatalf("helper env = %q", env)
	}
}

func TestPluginRestartWaitChannelReportsHelperFailure(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "restart-helper.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 9\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, done, err := launchPluginRestart(context.Background(), filepath.Join(dir, "state"), script, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 9 {
			t.Fatalf("helper exit = %v, want exit status 9", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("restart helper exit was not observed")
	}
}

// TestStopPrefersControlChannel checks the order the actions rely on: a
// listening daemon is asked politely and never signalled, and a daemon
// without a channel still gets SIGTERM.
func TestStopPrefersControlChannel(t *testing.T) {
	dir := testkit.ShortTempDir(t)
	ln, err := ListenControl(dir, nil)
	if err != nil {
		t.Fatalf("ListenControl: %v", err)
	}
	var mu sync.Mutex
	stops, resyncs := 0, 0
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		ServeControl(ctx, ln, ControlHandlers{
			Stop: func() {
				mu.Lock()
				stops++
				mu.Unlock()
			},
			Resync: func() {
				mu.Lock()
				resyncs++
				mu.Unlock()
			},
			Status: func() string { return "version=test pid=1 uptime=0s agents=0 dropped=0 herdr=ok" },
		}, nil)
	}()
	t.Cleanup(func() {
		cancel()
		<-served
	})

	p := NewProcess(dir, nil)
	p.exe = "/bin/sh"
	pid, err := p.Spawn(context.Background(), []string{"-c", "sleep 30"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(pid) })
	waitUntil(t, func() bool { return p.Alive(pid) }, "child alive")

	for _, verified := range []bool{false, true} {
		if err := p.Stop(pid, verified); err != nil {
			t.Fatalf("Stop = %v", err)
		}
		if err := p.Resync(pid, verified); err != nil {
			t.Fatalf("Resync = %v", err)
		}
	}
	line, err := p.Status(context.Background())
	if err != nil || line != "version=test pid=1 uptime=0s agents=0 dropped=0 herdr=ok" {
		t.Fatalf("Status = %q, err = %v", line, err)
	}
	mu.Lock()
	gotStops, gotResyncs := stops, resyncs
	mu.Unlock()
	if gotStops != 2 || gotResyncs != 2 {
		t.Fatalf("handlers called stop=%d resync=%d", gotStops, gotResyncs)
	}
	// The control channel answered, so no signal reached the child.
	time.Sleep(50 * time.Millisecond)
	if !p.Alive(pid) {
		t.Fatal("child was signalled although the control channel answered")
	}
	// Losing a previously responsive channel must not authorize a signal.
	cancel()
	<-served
	if err := p.Resync(pid, false); !errors.Is(err, domain.ErrControlUnavailable) {
		t.Fatalf("unverified Resync without control = %v", err)
	}
	if err := p.Stop(pid, false); !errors.Is(err, domain.ErrControlUnavailable) {
		t.Fatalf("unverified Stop without control = %v", err)
	}
	if !p.Alive(pid) {
		t.Fatal("unverified child was signalled after control channel disappeared")
	}
}

// TestStatusWithoutControlChannel reports the sentinel the supervisor
// checks for.
func TestStatusWithoutControlChannel(t *testing.T) {
	p := NewProcess(testkit.ShortTempDir(t), nil)
	if _, err := p.Status(context.Background()); !errors.Is(err, domain.ErrControlUnavailable) {
		t.Fatalf("Status = %v, want ErrControlUnavailable", err)
	}
}

// TestStateDirCreatedPrivate: the spawn and the control socket run before
// any state store, so they create the state directory on a fresh install;
// it holds the mapping, the logs and the control socket and must be the
// user's alone.
func TestStateDirCreatedPrivate(t *testing.T) {
	base := testkit.ShortTempDir(t)
	spawnDir := filepath.Join(base, "spawn")
	p := NewProcess(spawnDir, nil)
	p.exe = "/bin/sh"
	pid, err := p.Spawn(context.Background(), []string{"-c", "exit 0"})
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return !p.Alive(pid) }, "child exit")
	ctlDir := filepath.Join(base, "ctl")
	ln, err := ListenControl(ctlDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	for _, dir := range []string{spawnDir, ctlDir} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("%s mode = %04o, want 0700", dir, perm)
		}
	}
}
