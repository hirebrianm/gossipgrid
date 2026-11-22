/* GossipGrid web UI. Vanilla JavaScript, no frameworks, no CDN.
   Communicates with the Go server via fetch. On a 401 it redirects to /login.
   On a 503 it shows an "unavailable" banner. CSRF token is read from the
   gossipgrid_csrf cookie and sent in the X-CSRF-Token header on state-changing
   requests. */

(function () {
  "use strict";

  // CSRF cookie name (matches the Go server's auth.CSRFCookieName).
  var CSRF_COOKIE = "gossipgrid_csrf";

  // Poll interval for the diagnostics (ms).
  var DIAG_POLL_MS = 10000;

  // --- Helpers ---

  function getCookie(name) {
    var m = document.cookie.match(new RegExp("(?:^|; )" + name + "=([^;]*)"));
    return m ? m[1] : "";
  }

  function showBanner(kind, text) {
    var el = document.getElementById("banner");
    if (!el) return;
    el.className = "banner show " + kind;
    el.textContent = text;
  }

  function hideBanner() {
    var el = document.getElementById("banner");
    if (!el) return;
    el.className = "banner";
    el.textContent = "";
  }

  // apiFetch wraps fetch with CSRF header injection for state-changing methods
  // and a 401 redirect. It returns the Response (the caller inspects status).
  function apiFetch(path, opts) {
    opts = opts || {};
    var headers = opts.headers || {};
    var method = (opts.method || "GET").toUpperCase();
    // State-changing methods (POST/PUT/DELETE) require the CSRF header.
    if (method !== "GET" && method !== "HEAD") {
      var token = getCookie(CSRF_COOKIE);
      if (token) {
        headers["X-CSRF-Token"] = token;
      }
    }
    opts.headers = headers;
    return fetch(path, opts).then(function (res) {
      if (res.status === 401) {
        // Unauthenticated: redirect to the login page (only for browser
        // navigation, not for the login POST itself).
        if (path !== "/login") {
          window.location.href = "/login";
        }
      }
      return res;
    });
  }

  // --- Status pill ---

  function updateStatusPill(d) {
    var pill = document.getElementById("status-pill");
    var text = document.getElementById("status-text");
    if (!pill || !text) return;
    var ready = d && d.ready;
    var state = (d && d.lifecycle_state) || "";
    if (ready) {
      pill.className = "status-pill ready";
      text.textContent = "ready" + (state ? " (" + state + ")" : "");
    } else {
      pill.className = "status-pill unready";
      text.textContent = (state || "unready") + (d && d.lifecycle_reason ? " — " + d.lifecycle_reason : "");
    }
  }

  // --- Messages ---

  function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function loadMessages() {
    apiFetch("/api/messages?limit=50").then(function (res) {
      if (res.status === 503) {
        showBanner("error", "Service unavailable (the replica is not ready to serve messages).");
        document.getElementById("msg-count").textContent = "Unavailable";
        return;
      }
      if (!res.ok) {
        showBanner("error", "Failed to load messages (HTTP " + res.status + ").");
        return;
      }
      hideBanner();
      return res.json();
    }).then(function (data) {
      if (!data) return;
      var list = document.getElementById("msg-list");
      var empty = document.getElementById("msg-empty");
      var count = document.getElementById("msg-count");
      if (count) count.textContent = data.total + " message(s)";
      list.innerHTML = "";
      var msgs = data.messages || [];
      if (msgs.length === 0) {
        empty.hidden = false;
        return;
      }
      empty.hidden = true;
      msgs.forEach(function (m) {
        var li = document.createElement("li");
        var idEl = document.createElement("div");
        idEl.className = "msg-id";
        idEl.textContent = m.id;
        var bodyEl = document.createElement("div");
        bodyEl.className = "msg-body";
        bodyEl.textContent = m.body;
        var metaEl = document.createElement("div");
        metaEl.className = "msg-meta";
        var originSpan = document.createElement("span");
        originSpan.className = "msg-origin";
        originSpan.textContent = "from " + m.origin;
        metaEl.appendChild(originSpan);
        metaEl.appendChild(document.createTextNode(" · " + formatTime(m.created_at)));
        li.appendChild(idEl);
        li.appendChild(bodyEl);
        li.appendChild(metaEl);
        list.appendChild(li);
      });
    }).catch(function (e) {
      showBanner("error", "Network error: " + e.message);
    });
  }

  function formatTime(s) {
    if (!s) return "";
    try {
      var d = new Date(s);
      return d.toLocaleString();
    } catch (e) {
      return s;
    }
  }

  function submitMessage() {
    var body = document.getElementById("msg-body").value.trim();
    if (!body) {
      showBanner("warn", "Message body must not be empty.");
      return;
    }
    var btn = document.getElementById("msg-submit");
    btn.disabled = true;
    var csrf = getCookie(CSRF_COOKIE);
    fetch("/api/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrf
      },
      body: JSON.stringify({ body: body })
    }).then(function (res) {
      if (res.status === 401) { window.location.href = "/login"; return; }
      if (res.status === 403) {
        showBanner("error", "CSRF token missing or invalid. Refresh the page and try again.");
        btn.disabled = false;
        return;
      }
      if (res.status === 503) {
        showBanner("error", "Service unavailable: the replica is not ready to accept messages.");
        btn.disabled = false;
        return;
      }
      if (!res.ok) {
        return res.json().then(function (d) {
          showBanner("error", (d && d.error) || ("HTTP " + res.status));
        }).catch(function () {
          showBanner("error", "HTTP " + res.status);
        });
      }
      hideBanner();
      document.getElementById("msg-body").value = "";
      loadMessages();
    }).catch(function (e) {
      showBanner("error", "Network error: " + e.message);
      btn.disabled = false;
    });
  }

  // --- Peers ---

  function loadPeers(d) {
    var meta = document.getElementById("peer-meta");
    var list = document.getElementById("peer-list");
    var empty = document.getElementById("peer-empty");
    if (meta) {
      var n = d.peer_count || 0;
      var expected = d.expected_replicas || 0;
      meta.textContent = n + " peer(s) discovered" + (expected ? " of " + expected + " expected" : "") + (d.discovery_stale ? " (stale)" : "");
    }
    if (!list) return;
    list.innerHTML = "";
    var peers = d.peers || [];
    if (peers.length === 0) {
      if (empty) empty.hidden = false;
      return;
    }
    if (empty) empty.hidden = true;
    peers.forEach(function (p) {
      var li = document.createElement("li");
      var idSpan = document.createElement("span");
      idSpan.className = "peer-id";
      idSpan.textContent = p.identity || p.hostname;
      var stateSpan = document.createElement("span");
      stateSpan.className = "peer-state " + (p.state || "found");
      stateSpan.textContent = p.state || "found";
      li.appendChild(idSpan);
      li.appendChild(stateSpan);
      list.appendChild(li);
    });
  }

  // --- Diagnostics ---

  function loadDiagnostics() {
    apiFetch("/api/diagnostics").then(function (res) {
      if (!res.ok) return null;
      return res.json();
    }).then(function (d) {
      if (!d) return;
      updateStatusPill(d);
      loadPeers(d);
      set("d-node", d.node_id);
      set("d-version", d.version);
      set("d-fips", d.fips_module || "n/a");
      set("d-fips-enabled", d.fips_enabled ? "yes" : "no");
      set("d-lifecycle", d.lifecycle_enabled ? d.lifecycle_state : "disabled");
      set("d-lifecycle-reason", d.lifecycle_enabled ? (d.lifecycle_reason || "n/a") : "n/a");
      set("d-peertls", d.peer_tls_enabled ? "enabled" : "disabled");
      set("d-cert-fp", d.peer_tls_enabled ? (d.peer_cert_fingerprint || "n/a") : "n/a");
      set("d-reload-count", d.peer_tls_enabled ? String(d.peer_reload_count) : "n/a");
      set("d-cred-state", d.auth_enabled ? d.credential_state : "disabled");
      set("d-cred-gen", d.auth_enabled && d.credential_generation ? String(d.credential_generation) : "n/a");
    }).catch(function () { /* network error; ignore */ });
  }

  function set(id, val) {
    var el = document.getElementById(id);
    if (el) el.textContent = val == null || val === "" ? "n/a" : val;
  }

  // --- Login (only on the login page) ---

  function initLogin() {
    var form = document.getElementById("login-form");
    if (!form) return;
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      var errEl = document.getElementById("login-error");
      errEl.className = "banner error";
      errEl.textContent = "";
      var btn = document.getElementById("login-submit");
      btn.disabled = true;
      var password = document.getElementById("password").value;
      fetch("/login", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "password=" + encodeURIComponent(password)
      }).then(function (res) {
        if (res.status === 503) {
          errEl.textContent = "Service unavailable: no valid credential loaded. Try again later.";
          btn.disabled = false;
          return;
        }
        if (res.status === 401) {
          errEl.textContent = "Invalid password.";
          btn.disabled = false;
          return;
        }
        if (res.ok || res.redirected) {
          window.location.href = "/";
          return;
        }
        errEl.textContent = "Login failed (HTTP " + res.status + ").";
        btn.disabled = false;
      }).catch(function (e) {
        errEl.textContent = "Network error: " + e.message;
        btn.disabled = false;
      });
    });
  }

  // --- Init ---

  function init() {
    if (document.getElementById("login-form")) {
      initLogin();
      return;
    }
    // Main app page.
    var msgForm = document.getElementById("msg-form");
    if (msgForm) msgForm.addEventListener("submit", function (e) { e.preventDefault(); submitMessage(); });
    var refresh = document.getElementById("msg-refresh");
    if (refresh) refresh.addEventListener("click", loadMessages);
    loadMessages();
    loadDiagnostics();
    setInterval(loadDiagnostics, DIAG_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
