package web

import (
	"errors"
	"io/fs"
	"net/url"
	"strings"
	"testing"
)

func TestPublicBundleHasPagesAndResourcesWithoutConfigurationOrDebugFiles(t *testing.T) {
	// A release must resolve the resources used by every public page. The
	// private neighbour checks are also exercised with a real ignored .env
	// fixture present during compilation by the portable release audit.
	for _, private := range []string{"static/.env", "static/service.env", "static/debug.txt"} {
		if _, err := Files.ReadFile(private); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("private neighbour embedded: %s: %v", private, err)
		}
	}
	for _, page := range []string{"static/index.html", "static/admin.html", "static/poll.html"} {
		data, err := Files.ReadFile(page)
		if err != nil || len(data) == 0 {
			t.Fatalf("missing release page %s: %v", page, err)
		}
		// Every local HTML resource reference must exist in the same binary.
		for _, field := range strings.Split(string(data), "\"") {
			if strings.HasPrefix(field, "/static/") {
				resource, err := url.Parse(field)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := Files.ReadFile(strings.TrimPrefix(resource.Path, "/")); err != nil {
					t.Errorf("%s references unavailable resource %s", page, field)
				}
			}
		}
	}
}
