package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExplicitMissingEnvironmentFailsBeforeStorageAndImplicitDefaultRemainsOptional(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit-default"
		if explicit {
			name = "explicit-missing-file"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			t.Chdir(directory)
			// Stop at config validation even if an implicit env file is absent;
			// this must never reach a database, file store or HTTP listener.
			t.Setenv("ADMIN_PASSWORD", "short")
			previousFlags, previousArgs := flag.CommandLine, os.Args
			flag.CommandLine = flag.NewFlagSet(name, flag.ContinueOnError)
			os.Args = []string{"gigaquizz"}
			if explicit {
				os.Args = append(os.Args, "-env", filepath.Join(directory, "missing.env"))
			}
			t.Cleanup(func() { flag.CommandLine, os.Args = previousFlags, previousArgs })
			err := run()
			if explicit {
				if err == nil || err.Error() != "cannot load environment file" {
					t.Fatalf("explicit missing configuration was ignored: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "ADMIN_PASSWORD") {
				t.Fatalf("missing implicit default prevented environment-only configuration: %v", err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("configuration failure created storage: %v", err)
			}
		})
	}
}
