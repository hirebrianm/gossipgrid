// Package auth implements Milestone 6's browser authentication: single-
// administrator password verification against a mounted versioned credential
// file, cross-replica signed session cookies, CSRF protection, and a poll-
// based credential reloader that rotates credentials without a process
// restart.
//
// The package is deliberately built only on the Go standard library so it
// stays compatible with the FIPS-enabled Go build profile (GOFIPS140). The
// cryptographic primitives used are all FIPS 140-3 approved:
//
//   - crypto/pbkdf2 (PBKDF2-HMAC-SHA-256, RFC 2898) for password hashing;
//   - crypto/hmac + crypto/sha256 for the session cookie signature;
//   - crypto/rand for the per-credential salt, the session signing key, the
//     session identifier, and the CSRF token;
//   - crypto/subtle for constant-time comparison of the verifier and the
//     cookie signature.
//
// No whole-application FIPS certification is claimed; only the FIPS-enabled
// Go module and the use of FIPS-approved primitives are documented.
//
// The credential file is the sole source of credential material. The
// plaintext password is never stored anywhere; the file holds only the
// versioned credential record (algorithm, format version, PBKDF2 iteration
// count, per-credential salt, PBKDF2 verifier, credential generation, and the
// common session-signing key). The file is supplied as a mounted read-only
// Kubernetes Secret volume in production and as an ordinary file in local
// development; it is never delivered through environment variables.
package auth
