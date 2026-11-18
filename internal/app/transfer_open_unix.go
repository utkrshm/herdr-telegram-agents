//go:build !windows

package app

import (
	"os"
	"syscall"
)

// O_NONBLOCK prevents a FIFO supplied as a path from hanging the bridge's
// transfer worker before it can inspect and reject the descriptor.
func openTransferFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
