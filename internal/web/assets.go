package web

import "embed"

// Files contains the application pages and their same-origin assets.
//
//go:embed static/*
var Files embed.FS
