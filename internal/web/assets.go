package web

import "embed"

// Files contains only the reviewed public pages and same-origin assets. Avoid a
// wildcard: an ignored .env or debug file placed beside them must never be
// compiled into the server and exposed through /static/.
//
//go:embed static/index.html static/admin.html static/poll.html
//go:embed static/app.css static/common.js static/admin.js static/poll.js
//go:embed static/qrcodegen.js static/qrcodegen.LICENSE.txt static/qrcodegen.SOURCE.md
var Files embed.FS
