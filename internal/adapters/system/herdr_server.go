package system

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const herdrDefaultSession = "default"

var (
	herdrServerReadyTimeout = 12 * time.Second
	herdrServerPollInterval = 250 * time.Millisecond
	herdrCommandTimeout     = 2 * time.Second
	herdrCommandWaitDelay   = 500 * time.Millisecond
)

type herdrServerStatus struct {
	Running bool `json:"running"`
}

// ControlHerdrServer controls only Herdr's explicitly named default session.
// Start waits until the read-only status command confirms the server is up.
func ControlHerdrServer(ctx context.Context, binary string, stop bool) error {
	if strings.TrimSpace(binary) == "" {
		return errors.New("Herdr executable path is empty")
	}
	if stop {
		_, err := runHerdrCommand(ctx, binary, "--session", herdrDefaultSession, "server", "stop")
		return err
	}
	return startHerdrServer(ctx, binary)
}

func startHerdrServer(ctx context.Context, binary string) error {
	status, err := readHerdrServerStatus(ctx, binary)
	if err == nil && status.Running {
		return nil
	}

	cmd := detachedCommand(ctx, binary, "--session", herdrDefaultSession, "server")
	// Its stdio points at the null device, so Wait has no inherited pipes to
	// drain; leave it waiting for the detached server instead of applying the
	// short deadline used for one-shot CLI calls.
	cmd.WaitDelay = 0
	// The server must outlive this request's bounded readiness context.
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open null device for Herdr server: %w", err)
	}
	defer devNull.Close()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devNull, devNull, devNull
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start herdr --session %s server: %w", herdrDefaultSession, err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- cmd.Wait() }()

	deadline := time.NewTimer(herdrServerReadyTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(herdrServerPollInterval)
	defer poll.Stop()
	var lastStatusErr error
	for {
		status, statusErr := readHerdrServerStatus(ctx, binary)
		if statusErr == nil && status.Running {
			return nil
		}
		if statusErr != nil {
			lastStatusErr = statusErr
		}
		select {
		case processErr := <-processDone:
			if processErr == nil {
				return errors.New("Herdr server process exited before its default-session socket became ready")
			}
			return fmt.Errorf("Herdr server process exited before its default-session socket became ready: %w", processErr)
		case <-deadline.C:
			terminateProcessGroup(cmd.Process.Pid)
			if lastStatusErr != nil {
				return fmt.Errorf("default Herdr server did not become ready within %s (last status error: %v)", herdrServerReadyTimeout, lastStatusErr)
			}
			return fmt.Errorf("default Herdr server did not report running within %s", herdrServerReadyTimeout)
		case <-ctx.Done():
			terminateProcessGroup(cmd.Process.Pid)
			return fmt.Errorf("waiting for default Herdr server readiness: %w", ctx.Err())
		case <-poll.C:
		}
	}
}

func readHerdrServerStatus(ctx context.Context, binary string) (herdrServerStatus, error) {
	commandCtx, cancel := context.WithTimeout(ctx, herdrCommandTimeout)
	defer cancel()
	output, err := runHerdrCommand(commandCtx, binary, "--session", herdrDefaultSession, "status", "server", "--json")
	if err != nil {
		return herdrServerStatus{}, err
	}
	var status herdrServerStatus
	if err := json.Unmarshal(output, &status); err != nil {
		return herdrServerStatus{}, fmt.Errorf("decode default Herdr server status: %w", err)
	}
	return status, nil
}

// runHerdrCommand sends output to a temporary file rather than os/exec's
// pipes: a CLI descendant inheriting stdout cannot hold Wait open. The
// command helper keeps Windows console behavior consistent with other calls.
func runHerdrCommand(ctx context.Context, binary string, args ...string) ([]byte, error) {
	outputFile, err := os.CreateTemp("", "herdr-server-control-*.log")
	if err != nil {
		return nil, fmt.Errorf("create Herdr command output file: %w", err)
	}
	path := outputFile.Name()
	defer os.Remove(path)
	cmd := command(ctx, binary, args...)
	cmd.Stdout, cmd.Stderr = outputFile, outputFile
	cmd.WaitDelay = herdrCommandWaitDelay
	setCommandProcessGroup(cmd)
	err = cmd.Run()
	if err != nil && cmd.Process != nil {
		terminateProcessGroup(cmd.Process.Pid)
	}
	if _, seekErr := outputFile.Seek(0, io.SeekStart); seekErr != nil && err == nil {
		err = seekErr
	}
	const outputLimit = 64 << 10
	output, readErr := io.ReadAll(io.LimitReader(outputFile, outputLimit))
	if closeErr := outputFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if readErr != nil && err == nil {
		err = readErr
	}
	if err != nil {
		text := strings.TrimSpace(string(output))
		if text != "" {
			return output, fmt.Errorf("herdr --session %s %s failed: %w: %s", herdrDefaultSession, strings.Join(args[2:], " "), err, text)
		}
		return output, fmt.Errorf("herdr --session %s %s failed: %w", herdrDefaultSession, strings.Join(args[2:], " "), err)
	}
	return output, nil
}
