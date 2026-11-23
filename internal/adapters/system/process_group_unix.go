//go:build !windows

package system

import (
	"os"
	"os/exec"
	"syscall"
)

func setCommandProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		terminateProcessGroup(cmd.Process.Pid)
		return nil
	}
}

func terminateProcessGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}
