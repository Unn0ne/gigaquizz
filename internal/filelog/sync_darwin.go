//go:build darwin

package filelog

import (
	"os"
	"syscall"
)

func SyncMode() string { return "darwin_f_fullfsync" }

func durableSync(f *os.File) error {
	// F_FULLFSYNC is 51 in the Darwin SDK. Plain fsync is deliberately not a
	// fallback: it does not provide the same device-cache flush contract.
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), 51, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
