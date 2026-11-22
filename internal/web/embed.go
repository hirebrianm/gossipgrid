package web

import "embed"

// staticFS embeds the web UI's static assets (index.html, login.html, app.js,
// styles.css) into the Go binary. The UI is lightweight: no external
// frameworks, no CDNs, no external assets. All files are served from the
// embedded filesystem so the binary is self-contained.
//
// The go:embed directive requires the matched files to exist at build time;
// the static/ directory is a sibling of this file.
//
//go:embed static/index.html static/login.html static/app.js static/styles.css
var staticFS embed.FS
