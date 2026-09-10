//go:build linux

package filelog

import "os"

func SyncMode() string             { return "linux_fsync" }
func durableSync(f *os.File) error { return f.Sync() }
