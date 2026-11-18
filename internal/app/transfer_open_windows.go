//go:build windows

package app

import "os"

func openTransferFile(path string) (*os.File, error) { return os.Open(path) }
