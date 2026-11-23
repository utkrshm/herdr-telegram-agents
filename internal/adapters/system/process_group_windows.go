//go:build windows

package system

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func setCommandProcessGroup(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		terminateProcessGroup(cmd.Process.Pid)
		return nil
	}
}

func terminateProcessGroup(pid int) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cmd := command(ctx, "taskkill", "/PID", strconv.Itoa(pid), "/T", "/F")
	cmd.WaitDelay = 250 * time.Millisecond
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err == nil {
		cmd.Stdout, cmd.Stderr = devNull, devNull
		err = cmd.Run()
		_ = devNull.Close()
	}
	if err != nil {
		_ = process.Kill()
	}
}
