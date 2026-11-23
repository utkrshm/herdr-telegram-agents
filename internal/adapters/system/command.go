package system

import (
	"context"
	"os/exec"
)

// command is exec.CommandContext for every short-lived child this package
// starts (git, opencode, herdr, the URL opener). On Windows the child gets
// no console window: the daemon runs detached without a console, so any
// console program it starts would otherwise open a new visible terminal
// window. The daemon itself is started by Process with DETACHED_PROCESS
// instead. scripts/check-imports.sh keeps other exec.Command calls out of
// this package.
func command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	hideConsole(cmd)
	return cmd
}

// detachedCommand uses the standard command setup but gives the child its
// own process session and lets it outlive the caller's readiness context.
func detachedCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := command(ctx, name, args...)
	cmd.SysProcAttr = detachAttr()
	cmd.Cancel = func() error { return nil }
	return cmd
}
