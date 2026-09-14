package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestInvalidPartitionBoundDoesNotCreateStorage(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "must-not-exist")
	store, err := New(context.Background(), Config{Directory: directory, MaxUnique: 1, MaxPartitionUnique: 120000000})
	if err == nil {
		store.Close()
		t.Fatal("inconsistent partition bound accepted")
	}
	if _, err := os.Lstat(directory); !os.IsNotExist(err) {
		t.Fatalf("invalid configuration touched storage: %v", err)
	}
}
