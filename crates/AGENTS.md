# crates/AGENTS.md: backend rules

The root `AGENTS.md` applies. Numbers, gates and exceptions live in root `REQUIREMENTS.md`.
Read `docs/STATE-AND-CONCURRENCY.md` for changes to ownership, persistence, publication, callbacks or shutdown. Paths here are relative to the repository root.

## Backend

Current crate responsibilities:

- `crates/ck-stream`: shared mmap pool, positions, publication, reader leases and cache retention. No workspace dependencies or transport/provider work.
- `crates/ck-core`: legacy config import, runtime projection and credential formats. No networking.
- `crates/ck-wire`: provider-neutral, Go-exact helpers shared by translators and executors.
- `crates/ck-translate`: format translation between the Anthropic, OpenAI and Gemini APIs.
- `crates/ck-exec`: one module per upstream provider (wire format, auth headers, HTTP client profile).
- `crates/ck-server`: axum routes, client auth, credential selection, management API.
- `crates/cooker`: the binary.

A workspace reorganization updates this list in the same PR.

S1 freezes the responsibility boundary in `docs/INTERNAL-STREAM.md`, §2.1. `ck-stream` owns storage and position-backed views; it owns no HTTP, credentials, provider dispatch or durable state. `ck-core` keeps configuration, credential and model types; `ck-wire` keeps boundary helpers. Codecs and capability validation belong in `ck-translate`; execution envelopes and provider transports belong in `ck-exec`. Client boundaries, scheduling, resume authorization and runtime-owned producers belong in `ck-server`; `cooker` remains bootstrap.

Phase B installs `ck-stream` without production callers. Later cutover phases add each consuming crate's allowed dependency together with its actual use. Add it to the binary's allowed dependencies only if the binary uses it directly. Keep the existing table parser and exact-membership checks.

### APIs and modularity

- Keep API abstractions and responsibility boundaries clear. URLs, paths, configuration, upstream fees, limits and rules are data rather than hardcoded policy. Keep endpoints working.
- Inside the router, every transport protocol is represented as one SSE event stream in an internal format that extends OpenAI Chat Completions chunks with extra attributes, carried in reserved `x_ck` JSON fields and named `event:` events under one versioned grammar. Unknown fields and events are kept and passed to egress. Convert to it only at ingress and from it only at egress, including for same-protocol requests; no raw-byte passthrough path. Internal stages process the unbounded stream incrementally and never wait for a complete body.
- The internal stream is stored in one process-owned mmap pool, never in heap buffers. Fixed-size frames hold the bytes; a position is a frame number and offset scoped by stream generation. Stages hold positions, not body copies or retained raw pointers. The pool is private to the process user and closes only after owned work and leases end. A response stays cached across completion and disconnect until expiry or eviction under `docs/INTERNAL-STREAM.md`, §6; request/scratch ranges may release after their last dependent reader.
- Non-SSE payloads (images, audio, video, realtime) travel in the same stream as custom data in fixed chunks of at most 50 KB, flushed within 100 ms.
- A rewrite replaces the old code in the same change. Do not keep old and new paths side by side, adapters to the old form or unreachable legacy code.
- Case mapping and case-insensitive comparison use `ck_core::gostr::GoStr`: `go_lower`, `go_upper`, `go_eq_fold`, matching Go's `strings.ToLower`, `strings.ToUpper` and `strings.EqualFold`. `clippy.toml` disallows Rust's full `str::to_lowercase` and `str::to_uppercase` mappings. Use `eq_ignore_ascii_case` only for ASCII-defined inputs such as headers and protocol tokens, or explicitly ASCII comparisons. An ASCII literal alone is insufficient: Go folding also matches `ſ` with `s` and the Kelvin sign with `k`.

### Compatibility

- cooker is a full rewrite and keeps no backward compatibility: config keys, database schema, CLI flags, environment variables, routes, response shapes, file paths, crate names and log targets change whenever a better design needs it. The ruling is in `REQUIREMENTS.md`, "Exceptions".
- Change every caller in the same PR: dashboard, TUI, tests, docs, scripts and installers. Leave no fallback, alias or shim for the old form.
- Existing import features, such as first import of a CLIProxyAPI YAML config, stay only while they are useful and carry no compatibility guarantee.
- Remove a replaced management route in the same PR as its replacement. The Go-era `/v0/management` and `/v8/management` families and the existing `/admin/v1` routes are removed as their areas migrate; they are not versions of the new `/admin/v0` API. Prove removal against the actual router, including fallback paths. Follow the root caller-migration rule.

### Logging

- Use one log format and meaningful debug, info, warn and error levels. Expected cases are not errors; failures are not silently swallowed.
- Apply root Secrets rules. Upstream request and response capture goes through `crates/ck-server/src/observability/request_logging.rs`. It redacts `Cookie` and `Set-Cookie`, URL credentials and short keys, and spools large sections to disk. Do not format or log captured bytes elsewhere.

### Security (if present)

- Every endpoint checks authorization or is explicitly public at its route definition with a reason. Validate input at the boundary; apply CSRF, CORS and rate limits where needed. [rule: authz]
- Validate management `Host` and reject a cross-site `Origin` or `Sec-Fetch-Site` before comparing the key, so a web page cannot drive the local API or trigger the loopback ban. [rule: management-cross-site]
- End-user errors exclude upstream/internal vendor names, ids and raw messages. Operators running the upstream connection may see that detail for troubleshooting.
- Refuse or mask card numbers, secrets and tokens at each free-text write boundary, with a database constraint as backstop. No log, including database logs, echoes refused values. [rule: sensitive-values]
- Load credential and key files through one path checking owner and mode, rather than ad hoc reads or plain environment variables where other credentials use files.
- Define and test recovery from every lockout or ceiling. Admin recovery invalidates old credentials and opens the account only through its own recovery step.
- Known and unknown accounts receive the same answers and do the same work.
- Exchange the dashboard's management key for a server-issued, server-revocable HttpOnly, SameSite=Strict session cookie, with Secure over HTTPS. Revoke the session on sign-out. Require a same-origin check using Origin or Sec-Fetch-Site for cookie-authenticated unsafe methods. TUI/CLI keep Bearer authentication.
- Mask ordinary management reads, revisions, effective config, credential-list API-key account values and per-key usage. Explicit secret exports, credential downloads and usage queue/RESP records retain their documented plaintext access.
- Bind retained secrets to their destination; changing it requires plaintext re-entry.

### Money and state (if present)

Money clauses apply only where billing or a ledger exists.

- Money-moving operations are idempotent under concurrent requests and out-of-order callbacks. [rule: money-idempotent]
- Other operations a client, upstream or job can retry are idempotent; the rest remain concurrency-safe.
- A ledger balances to every balance it feeds and supports replay/reconciliation. [rule: ledger]
- Do not close accounts or records while money is in motion.
- SQLite is the sole durable authority for configuration and OAuth credentials. YAML is an explicit first-import format, not a watched runtime file or an asynchronously mirrored backup. Config mutations use management resources and atomic revisions. Preserve OAuth-only scope on import; do not silently discard unsupported input. Credential state is separate from config revisions: restoring configuration must never resurrect a revoked token.
- One running process owns a database and serves its state from memory; offline commands that write it are refused while it runs, and online changes go through the management API. The `--db` file is a standalone SQLite snapshot without WAL, SHM or journal companions, published atomically before a commit succeeds, so copying it while the server runs is a complete backup; `config backup` encodes the accepted state into a new file. Keep snapshots, temporary working databases and backups private; logical deletion is not a promise of physical erasure from disk. Preserve the invariants in `docs/STATE-AND-CONCURRENCY.md`.
- Keep config commits and credential writes atomic against concurrent writers, runtime publication and token refresh. Serialize refresh-token rotation across processes sharing a database so a refresh token is never spent twice.
- A request uses one snapshot even if configuration or credentials change during it.

### Background work (if present)

- Contain failing or panicking rounds: log the job name, raise an alert and keep the service and other jobs running.
- A background task or thread stops with its owner. Stop it on drop or shutdown and wait for it before deleting directories it can still write; otherwise tests fail with `DirectoryNotEmpty`.
- Wait on events (channels, `Notify`, file watchers), not polling timers. Keep the idle server within the limits in `REQUIREMENTS.md`, "Numbers".

### Operations (if present)

- cooker is one service, not a cluster. When memory, disk or file descriptors run out, fail visibly so downstream services see the error, and leave recovery to the operating system and the supervisor. Add no degradation or fallback path for resource exhaustion; degradation belongs to the cluster. Choosing another upstream or credential after an upstream failure is routing, not degradation.
- Deploy production only from merged `master`; an override skipping that check must refuse the production target. Approval follows root Rule priority.
- Backups need a written restore procedure covering work since the last backup and the location, never values, of required key files. Preserve the single-file recovery contract above.

### Admin panel (if present)

- Minimize maintenance burden and cut unnecessary features rather than over-engineering.
- Every setting changed while the service runs has an admin page. Deploy-only values stay in config. Operations listed with a reason in `ui/tools/api-allowlist.json` need no page, subject to the exceptions in `REQUIREMENTS.md`.

### Performance

- Watch storage, CPU and memory against the baseline; add no unnecessary dependencies.
- Keep request preparation and config loading linear in input size. Walk JSON or YAML once instead of rescanning from the start for each element; use path-based JSON edits as in `crates/ck-exec/src/claude/signals.rs`.
- Prompt-cache changes preserve request semantics and stable prefixes, and show cache-read and cache-write usage for warm, changed-context and idle continuations before merge. Never add inference keepalive pings. `bench/claude-cache-live.sh` performs this proof by hand against a live Claude login, subject to root live-upstream approval.

### Verification

- Use isolated VMs or network namespaces with disposable data when needed. Verify storage behaviour against a real private database; builds and unit tests alone do not prove it.
- Tests use local mock upstreams and never send traffic to real provider accounts. Give every test a private SQLite database and a private `auth-dir` for its credential files.
- Build test runtimes with `ck_server::testing::runtime`. `ck_server::testing` compiles only in tests and with the `testing` feature enabled by dev-dependencies and harnesses.
- The full suite is slow and memory-heavy because of BoringSSL and release linking. During iteration run the affected crate or filter, for example `cargo test -p ck-exec kimi`. CI runs fmt, clippy with warnings denied and the full suite. On shared or small machines use `CARGO_BUILD_JOBS=2 nice -n 19`.
- Go comparison tests run fully only in CI's network-denied namespace. For local Linux differential checks use `./harness/run`; see `harness/README.md`.

### Cargo workspace layout

1. Keep a virtual root manifest and flat `crates/<name>/` directories whose names match their crates. `Cargo.toml` is the current member list.
2. Name new crates by responsibility using one naming convention, rather than common, util, helper or misc.
3. Put new management API code in the crate that owns management routes. Set boundaries from callers, not speculative interfaces. Keep provider execution separate from translation and organize executors by provider.
4. Begin a new crate's `lib.rs` with its responsibility, allowed dependencies and prohibited work. Update architecture documentation before adding a crate.
5. Record allowed dependency directions in one table and check actual workspace metadata against it in CI. Prove the check fails on a forbidden edge.

   Allowed dependency directions. `tools/check-crate-deps.py` compares every normal, build and dev edge between workspace crates, whatever its feature or target condition, with this table; a crate may also name itself as a dev dependency for its test features.

   | Crate | May depend on |
   | --- | --- |
   | `ck-stream` | none |
   | `ck-core` | none |
   | `ck-wire` | `ck-core` |
   | `ck-translate` | `ck-core`, `ck-wire`, `ck-stream` |
   | `ck-exec` | `ck-core`, `ck-wire`, `ck-translate` |
   | `ck-server` | `ck-core`, `ck-wire`, `ck-translate`, `ck-exec` |
   | `cooker` | `ck-core`, `ck-wire`, `ck-translate`, `ck-exec`, `ck-server` |

6. Keep unit tests beside modules and crate integration tests in `crates/<name>/tests/`. Keep Go recordings and fixtures excluded from published packages with `exclude = ["tests/"]`.

### Management API contract

Routes under `/admin/v0` follow this contract; v0 is EXPERIMENTAL.

- Return named collection envelopes with relative `next` links and single resources directly. Use limit/marker pagination and return 400 for unknown filters.
- Every state-changing operation declares its precondition authority or an explicit exemption. Use the resource revision for resource writes, the global config revision for apply/import/restore, and the credential-document hash for credential writes. Configuration writes missing a required precondition return 428; stale preconditions return 412. Clearing logs and cancelling sign-in are exempt. Verify missing conditions, stale conditions and atomic failure of multi-resource writes.
- Generate `X-Request-Id` in the outermost layer before access logging. Logs, response headers and error bodies carry the same value, including authorization failures, unknown routes and shutdown errors.
- Every operation has an operationId, tag, summary and description. Describe every parameter and field.

### OpenAPI drift

Generate `api/openapi.json` from Rust request and response types with utoipa, and dashboard API types from that document with openapi-typescript. CI regenerates both and rejects drift; edit the types, not the generated files.

### Management response schemas

Contract tests exercise every documented operation and validate actual response bodies and required headers against the generated contract. An operation without coverage fails the check. Generate an example for every response status from those responses and check example drift. Prove that a changed field shape and a missing required header each fail the check.

### Management API version

Send `Ck-API-Version: 0.N` in requests and responses; reject a mismatch with 406 and `api.version_mismatch` before mutation. A change to the generated OpenAPI document increments N and adds a `changes/<branch-slug>.md` fragment. Unauthenticated `GET /admin` reports v0 as EXPERIMENTAL and the current version; it returns 404 when management is disabled.

### Management error catalog

Use one error type and schema: an `errors` array with request_id, code, status, title, detail and links, plus pointer, pointers and resource where applicable. Register every published code, including authorization failures, in the catalog and keep its name stable. Clients classify errors by code, not message text.

### Dashboard artifact integrity

Backend development builds and tests work without dashboard assets. Release, Docker and installer builds require the real dashboard and fail if it is missing. Validate every embedded asset against its per-file SHA-256 manifest; a missing or mismatched asset fails the check.
