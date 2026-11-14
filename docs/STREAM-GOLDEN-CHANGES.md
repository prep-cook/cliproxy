# Stream golden changes and acceptance inventory

S1 replaces pair translation with boundary codecs. This ledger records changed contracts without regenerating Go expectations. Phase A freezes the input inventory at post-W1 `b518060ea1e43ef4b266591986ca92321e6289b5`; it does not claim S1 semantic coverage has passed.

## Ledger rules

Keep existing fixture inputs and their provenance. Keep exact-byte assertions for decoded media and opaque signatures. Replace wire-shape assertions only when the cited contract changes. Do not bless a wholesale rewrite of expected output.

A fixture ID is its repository path plus its recorded `name` and occurrence index. For files without names, use the JSON pointer/index. An inline case uses its file and test symbol. Record one row per changed case when its codec lands. A file-level group below is an inventory, not permission to skip its individual cases.

| Fixture ID | Route / direction / mode | Old behavior | Required behavior | Authority | Replacement semantic assertion | Status / evidence |
| --- | --- | --- | --- | --- | --- | --- |
| `crates/ck-translate/tests/golden.rs::fragmented_sse_keeps_tool_state_request_local` | Chat client / Claude response decode and Chat encode / stream | One buffered tool chunk at block stop; empty state after a separate stop | Start, each argument fragment, then end; request-local state | INTERNAL-STREAM §3.3, §3.6, §9.1; L03 | Observe the first argument fragment before block stop; preserve call ID and exact argument sequence under every input fragmentation | Required in Phase C; not implemented |
| `crates/ck-translate/tests/golden.rs::openai_passthrough_is_byte_oriented` | Chat same-protocol / request and response / stream | Invalid UTF-8 passes unchanged | Client 400 or upstream terminal error; binary media remains valid | INTERNAL-STREAM §2 C2, §3.7, §7, §9.1 | Invalid text never reaches upstream or emits success; valid multibyte text survives split reads; arbitrary decoded media bytes remain exact | Required in Phase C; not implemented |
| `crates/ck-translate/tests/golden.rs::registration_matches_go` | Same-protocol Claude and pair count wiring | Missing registry entries use model-only/raw fallback | Every accepted boundary converts through the internal stream | INTERNAL-STREAM §2 Conversion, §9.1 | Delete registry-wiring assertions; exercise same-protocol field preservation, validation and count semantics through codecs | Required in Phase C; not implemented |

No fixture expectations change in Phase A. Later rows must include a test name, command/result and commit, or an explicit unsupported-operation reason. Empty output, a missing registry arm and a skipped test are not semantic passes. Existing `golden.rs::PENDING` entries remain input obligations; they are not accepted losses.

## Chat reference checkpoints

The old pair registry and its goldens remain unchanged until integration cutover. These new boundary tests prove individual Chat contracts, not full reference-codec acceptance.

| Test in `crates/ck-translate/tests/chat_codec.rs` | Contract and observed result | Commit |
| --- | --- | --- |
| `completed_request_fields_and_messages_publish_before_the_body_ends` | Completed root fields and messages publish before EOF; parent containers precede children and pointers are assigned once. | `1405cb20` |
| `late_destination_rejection_keeps_published_prefix_without_success` | A late unsupported semantic preserves the canonical prefix but emits no request.end or done and authorizes no upstream body. | `1405cb20` |
| `completed_message_media_is_available_before_later_request_fields` | Media for a completed message is available before later root fields arrive. This does not prove flushing inside an unfinished media string. | `1405cb20` |
| `request_controls::native_controls_custom_declarations_and_nullable_values_survive_fence` | Native nullable controls, custom declarations and simultaneous sampling values survive the capability fence. | `04b6c056` |
| `request_controls::late_invalid_native_semantics_never_publish_request_end` | Invalid late native controls cannot authorize upstream encoding. | `04b6c056` |
| `response_media::native_audio_fragments_reassemble_across_every_base64_boundary` | Native audio reconstructs exact decoded bytes across source-event quartet boundaries in both downstream modes. | `04b6c056` |
| `response_media::unfinished_native_audio_cannot_publish_successful_choice_finish` | An unfinished audio quartet produces an error without successful item or response completion. | `04b6c056` |

At `1405cb20`, fmt check, clippy with warnings denied, and `cargo nextest run -p ck-translate -p ck-stream --locked --no-fail-fast` passed: 149 tests passed, one existing storage test skipped. Unfinished-string/media publication, full Chat media/provenance/usage/terminal coverage and final independent review remain acceptance obligations.

At `04b6c056`, the same fmt, clippy and two-crate nextest gates passed: 170 tests passed, one existing storage test skipped. The intervening media checkpoint also covers structured content, native media, recursive response provenance, usage details and malformed terminals. Full acceptance still requires unfinished-string publication/source retirement, complete request controls and provenance, final-snapshot reconciliation, stable logical-media identity, mutation/deletion semantics and independent final-tree review.


## Registry inventory: 30 arms

Source: `crates/ck-translate/src/lib.rs:283-315`. Each row names one current `(client, upstream)` arm, not a new pair API. `openai` means Chat; `openai-response` means Responses; `codex` and `antigravity` name wire envelopes, not extra client routes.

Fixture paths are `crates/ck-translate/tests/fixtures/pairs/<stem>.json`. Counts below are recorded cases by `path`: R=request, C=request_compat, E=request_envelope, S=stream, B=non_stream, T=token_count. A dash means no recorded cases, not unsupported semantics. Every row has request decode/upstream encode and upstream decode/client encode obligations, including buffered and streamed delivery. Count applicability comes from the operation/provider tables below, not a missing token-count renderer.

| Client | Upstream | Fixture stem | R | C | E | S | B | T |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| openai | openai | openai-openai | 247 | - | - | 8 | 3 | - |
| openai | claude | openai-claude | 329 | 133 | - | 19 | 80 | - |
| openai-response | codex | openai-response-codex | 188 | - | - | 18 | 11 | - |
| openai-response | claude | openai-response-claude | 486 | 14 | - | 216 | 255 | - |
| claude | openai | claude-openai | 358 | 243 | - | 84 | 44 | 3 |
| gemini | gemini | gemini-gemini | 121 | - | - | 10 | 11 | 3 |
| openai | gemini | openai-gemini | 353 | - | - | 21 | 23 | - |
| claude | gemini | claude-gemini | 339 | 243 | - | 44 | 72 | 3 |
| openai | codex | openai-codex | 381 | - | - | 62 | 93 | - |
| openai-response | gemini | openai-response-gemini | 380 | - | - | 236 | 155 | - |
| claude | codex | claude-codex | 417 | 254 | - | 91 | 80 | 3 |
| gemini | codex | gemini-codex | 152 | - | - | 22 | 24 | 3 |
| interactions | codex | interactions-codex | 54 | - | - | 21 | 19 | - |
| openai-response | openai | openai-response-openai | 346 | - | - | 171 | 78 | - |
| gemini | openai | gemini-openai | 153 | - | - | 12 | 14 | 3 |
| interactions | interactions | interactions-interactions | 49 | - | - | 4 | 6 | - |
| interactions | gemini | interactions-gemini | 180 | - | - | 17 | 23 | - |
| gemini | interactions | gemini-interactions | 170 | - | - | 12 | 12 | - |
| openai | interactions | openai-interactions | 283 | - | - | 25 | 30 | - |
| interactions | openai | interactions-openai | 72 | - | - | 16 | 10 | - |
| openai-response | interactions | openai-response-interactions | 212 | - | - | 451 | 99 | - |
| interactions | openai-response | interactions-openai-response | 78 | - | - | 23 | 17 | - |
| interactions | claude | interactions-claude | 139 | - | - | 19 | 30 | - |
| claude | interactions | claude-interactions | 318 | 245 | - | 22 | 50 | - |
| gemini | claude | gemini-claude | 214 | - | - | 18 | 38 | 3 |
| gemini | antigravity | gemini-antigravity | 172 | - | - | 19 | 20 | 3 |
| openai | antigravity | openai-antigravity | 362 | - | - | 35 | 13 | - |
| openai-response | antigravity | openai-response-antigravity | 330 | - | 20 | 29 | 37 | - |
| interactions | antigravity | interactions-antigravity | 195 | - | - | 28 | 28 | - |
| claude | antigravity | claude-antigravity | 583 | - | - | 92 | 83 | 3 |

These 30 files contain 12,143 cases. Native Claude→Claude is outside this registry and still requires full codec coverage. Native Responses and provider-native media/duplex paths also require conversion even when today's executor bypasses a pair. The 49 registration entries in `sdk_registry.json` enumerate registered and unregistered combinations; they are not 49 Rust registry arms.

## Existing fixture inventory

All paths below are relative to the repository root. Globs denote the complete matched set at the pinned baseline, including error and rejection cases. Preserve `tests/reference/` and its recorded source revisions; do not run its generators for S1 acceptance. Fixture presence is not proof that its old assertion meets the new contract.

| Input set | Existing contents and consumers | S1 treatment |
| --- | --- | --- |
| `crates/ck-translate/tests/fixtures/pairs/*.json` | 30 files above; `tests/golden.rs` | Preserve inputs; replace only ruled wire/loss assertions with semantic coverage |
| `crates/ck-translate/tests/fixtures/apply_patch_responses.json` | 139 scenarios; `tests/apply_patch_responses.rs` | Structured/custom argument history, incremental tools and terminal failures |
| `crates/ck-translate/tests/fixtures/claude_tool_search.json` | 72 fixtures; `golden.rs::tool_search_goldens` | Preserve namespaces, tool declarations and correlated results |
| `crates/ck-translate/tests/fixtures/codex_chat_go.json` | 7 requests, 4 non-stream and 3 stream cases; `src/codex_chat_response.rs` | Responses identities, signatures, media and terminal reconciliation |
| `crates/ck-translate/tests/fixtures/go_helpers.json` | 436 calls; `src/go_helper_tests.rs` | Keep contractual helper semantics; remove obsolete pair implementation assertions |
| `crates/ck-translate/tests/fixtures/sdk_registry.json` | 49 registrations and 42 vectors; `tests/sdk_translator.rs` | Replace fallback/wiring assertions; do not preserve the registry as architecture |
| `crates/ck-translate/tests/{registry_overlay,web_search_overlay}.rs` | Inline registry/capability/search cases | Migrate consumer-visible capabilities and search semantics, delete wiring-only checks |
| `crates/ck-exec/src/claude/testdata/*.json` | 9 files: `go_captures`, `go_executor`, `go_executor_tool_search`, `go_executor_v8.0.27`, `go_executor_v8.0.35`, `go_unit`, `go_unit_tool_search`, `go_unit_v8.0.27`, `go_unit_v8.0.35` | Preserve auth/cloak/cache preparation at the boundary; migrate payload, capture and usage assertions |
| `crates/ck-exec/tests/device_fixtures/devin/*.json` | 58 files; `src/devin_tests.rs` and related cases | Connect/protobuf/gzip envelopes, trailer order, all recorded client formats, count estimate, truncation and media refusals; auth-only files remain boundary tests |
| `crates/ck-exec/tests/device_fixtures/kimi/*.json` | 44 files; `src/kimi_tests.rs` and related cases | Claude delegation, Chat/Responses stream and buffered output, count, apply_patch, compact rejection, gzip/redirect and failure cases |
| `crates/ck-exec/tests/device_fixtures/meta/*.json` | 38 files; `src/meta_tests.rs` and related cases | Responses stream/buffered collection, count, compact rejection, usage and malformed/truncated output; retain auth-only coverage |
| `crates/ck-exec/tests/fixtures/*.json` | 35 files, grouped below | Preserve provider inputs and provenance; migrate transport/codec/accounting assertions |
| `crates/ck-server/tests/fixtures/*.json` | 24 files, grouped below | Route, capture, media, truncation and lifecycle inputs; unrelated management/model data remains unchanged |
| `crates/ck-server/tests/fixtures/lcp_go.jsonl.zst` | Scheduler trace corpus | Scheduling policy stays unchanged; migrate only execution-envelope callers |
| `crates/ck-server/tests/ws_fixtures/{ws_e2e,ws_vectors}.json` | WS end-to-end and vector corpus; `tests/ws_e2e.rs` | Continuation, turn identity, terminal output and disconnect ownership |
| `harness/fixtures.py`, `harness/evidence/{2026-10-02,2026-10-02-ultra-claude}.json.gz` | Differential request corpus and historical evidence | Keep input provenance; no regenerated Go expectations or claimed S1 pass |

The 35 executor fixture filenames, all under `crates/ck-exec/tests/fixtures/`:

| Family | Files |
| --- | --- |
| Google and relay | `aistudio_go.json`, `aistudio_go_v8.0.35.json`, `antigravity_go.json`, `antigravity_helpers_go.json`, `antigravity_replay_go.json`, `antigravity_v8_0_27_go.json`, `antigravity_v8_0_35_go.json`, `gemini_go.json`, `gemini_go_v8.0.35.json`, `vertex_go.json`, `vertex_go_v8.0.35.json` |
| Claude | `claude_ratelimit_go.json` |
| Codex | `codex_chrome_hello.json`, `codex_client_translate_go.json`, `codex_go.json`, `codex_go_v8.0.35.json`, `codex_live_go.json`, `codex_siwc_bind_go.json`, `codex_siwc_expiry_go.json`, `codex_siwc_go.json`, `codex_tokens_go.json`, `codex_tool_schema_go.json`, `codex_web_search_history_go.json`, `codex_ws_deflate_go.json`, `codex_ws_errors.json` |
| OpenAI-compatible | `openai_compat_go.json`, `openai_compat_go_v8.0.35.json` |
| xAI | `xai_auth_go.json`, `xai_go.json`, `xai_go_v8.0.35.json`, `xai_ws_go.json` |
| Shared boundary | `go_url_string.json`, `multipart_form_go.json`, `payload_media_go.json`, `proxy_go.json` |

The 24 server JSON fixture filenames, all under `crates/ck-server/tests/fixtures/`:

| Family | Files |
| --- | --- |
| Inference/media | `body_limit_go.json`, `codex_builtins_go.json`, `codex_tool_search_go.json`, `media_go.json` (74 scenarios), `speech_go.json` (31 scenarios), `passthrough_errors_go.json` (3 scenarios), `stream_truncation_go.json` (16 cases) |
| Realtime | `codex_live_tunnel_go.json`, `codex_live_ws_end_go.json`, `codex_live_ws_go.json`, `codex_live_ws_raw_go.json`, `realtime_http_go.json` (56 cases) |
| Capture/usage/dispatch | `capture_go.json`, `upstream_capture_go.json`, `observe_go.json`, `server_go.json`, `server_go_v8.0.35.json` |
| Adjacent unchanged input | `codex_models_go.json`, `config_diff_go.json`, `config_v8_auth_index_go.json`, `manage_go.json`, `model_definitions_go.json`, `quota_probe_go.json`, `safe_mode_go.json` |

Additional inline route acceptance inputs live in `crates/ck-server/tests/{routes,legacy_go,claude_passthrough,gemini_routes,openai_compat_routes,codex_alpha,codex_routed_images,duplex_dispatch,realtime,aistudio_relay,xai_usage_cancel,alloc_budget,scheduler_attempts}.rs` and the corresponding `src/inference/`, `src/realtime/` and executor unit tests. Existing names containing “passthrough” do not exempt their traffic from conversion. Core configuration, wire helper and binary CLI fixtures are outside codec replacement; retain their unchanged contracts when migrating callers.

## Route and mode inventory

Evidence: `crates/ck-server/src/lib.rs:60-101`, `src/inference/gemini.rs:42-45`, `src/inference/codex_alpha.rs:22-25`, `src/realtime/websocket.rs:65-66`, `src/realtime/api.rs:79-106`, `src/realtime/aistudio_relay.rs:49-59`. Paths after `src/` in this paragraph belong to `ck-server`.

Every accepted row requires all four boundary roles from INTERNAL-STREAM §5: client decode, upstream encode, upstream decode and client encode. JSON requests include embedded media and history. Streaming and buffered client modes must each be tested against every upstream delivery mode the provider uses. Unsupported mapped semantics fail at `request.end` with zero upstream sends; existing unsupported operations stay unsupported.

| Route / method | Operation and modes | Provider / fixture coverage input | Spec |
| --- | --- | --- | --- |
| POST `/v1/chat/completions` | Generate; JSON request; buffered JSON or SSE response | All applicable provider rows below; openai registry rows; `openai_compat_routes.rs`, `routes.rs` | §5.3–5.4 |
| POST `/v1/completions` | Legacy prompt/text/token-array forms; buffered or SSE, logprobs and choices | Generate providers via Chat shaping; `legacy_go.rs`; L12–L13 | §5.13–5.14 |
| POST `/v1/messages` | Messages; buffered or SSE, signatures/cache/tools | All applicable providers; claude registry rows and native Claude; `claude_passthrough.rs` | §5.1–5.2 |
| POST `/v1/messages/count_tokens` | Count; buffered JSON only | Provider count modes below; allocation and executor count fixtures | §5.15–5.16 |
| POST `/v1/responses`, `/backend-api/codex/responses` | Responses generation; buffered or SSE; encrypted/history/tool/media content | Responses registry rows and native Responses paths | §5.5–5.6 |
| POST `/v1/responses/compact`, `/backend-api/codex/responses/compact` | Compact; buffered only; reject `stream=true` | Supported compact providers below; preserve unsupported responses | §5.17–5.18 |
| GET `/v1/responses`, `/backend-api/codex/responses` | WS create/append, continuation, controls and response messages | Codex/xAI sessions and ordinary executor fallback; `ws_e2e.rs`, `duplex_dispatch.rs` | §5.7–5.8 |
| POST `/v1beta/models/{model}:generateContent` | Gemini buffered generation | Gemini registry rows and applicable providers; `gemini_routes.rs` | §5.9–5.10 |
| POST `/v1beta/models/{model}:streamGenerateContent` | Gemini SSE and non-SSE `alt` output; JSON/array framing | Same providers; test both egress encodings | §5.9–5.10 |
| POST `/v1beta/models/{model}:countTokens` | Count; buffered JSON | Provider count modes; native count wrapper/details | §5.15–5.16 |
| POST `/v1beta/interactions` | Model or agent; buffered/SSE, continuation, tools/media | Interactions registry rows; Gemini Interactions and Devin envelopes | §5.11–5.12 |
| POST `/v1/images/generations` | JSON request; buffered results or native/emulated SSE | Codex, OpenAI-compatible, xAI; `media_go.json`, `codex_routed_images.rs` | §5.19–5.20 |
| POST `/v1/images/edits` | JSON/multipart image and mask input; buffered/SSE output | Same image providers; multipart/payload-media fixtures | §5.19–5.20 |
| POST `/v1/audio/speech`, `/v1/tts` | Speech JSON input; raw audio output and media metadata | xAI; `speech_go.json`; L16 | §5.21–5.22 |
| POST `/v1/videos`, `/v1/videos/generations` | Native video create; JSON/media input, job JSON output | xAI; `media_go.json` | §5.23–5.24 |
| POST `/v1/videos/edits`, `/v1/videos/extensions` | Native video edit/extend; media references, job JSON | xAI; `media_go.json` | §5.23–5.24 |
| GET `/v1/videos/{request_id}` | Native job retrieval | xAI; job identity/status and affinity | §5.23–5.24 |
| POST `/openai/v1/videos` | OpenAI-shaped video create; JSON/multipart input | xAI; `media_go.json` | §5.23–5.24 |
| GET `/openai/v1/videos/{video_id}` | OpenAI-shaped job retrieval; buffered JSON | xAI; L17 status distinctions | §5.23–5.24 |
| GET `/openai/v1/videos/{video_id}/content` | Finite binary download | xAI; exact bytes/MIME, truncation and byte resume | §5.23–5.24 |
| POST `/v1/live`, `/v1/realtime`, `/v1/realtime/calls` | SDP offer/answer; bidirectional WebRTC packet media and data-channel controls | Codex Live; realtime fixtures and real loopback peers; both feature builds | §5.25–5.26 |
| GET `/v1/realtime` without `call_id` | Realtime WS JSON/binary, audio, controls and turns | Codex Live; `realtime.rs`, live WS fixtures | §5.25–5.26 |
| GET `/v1/live/{call_id}`, `/v1/realtime/calls/{call_id}`, `/v1/realtime?call_id=...` | Sideband WS on an existing call | Codex Live; live WS/tunnel fixtures; call ID is not a resume ID | §5.25–5.26 |
| POST `/v1/realtime/calls/{call_id}/hangup` | Explicit session close; cancel owned work | Codex Live; `realtime_http_go.json` | §4, §5.25 |
| POST `/v1/alpha/search`, `/backend-api/codex/alpha/search` | Alpha search request/result JSON | Codex; `codex_alpha.rs`; no raw-body exemption | §4, §5.26 |
| GET `/v1/ws` | AI Studio browser relay; JSON/binary request/response envelopes; S1 adds header-capable Fetch framing | `wsrelay::PATH`, Gemini codec, `aistudio_relay.rs`, AI Studio fixtures | §5.26, §6.7 |
| GET `/v1/streams?after=<cursor>` (S1 contract, not existing route) | Authenticated retained HTTP/WS delivery; WebRTC client-peer reattachment | Every preceding resumable output, including buffered/raw output and duplex directions | §6.7 |
| POST `/v1/streams` (S1 contract, not existing route) | Header-authenticated duplex input and client-peer signaling for Fetch clients; never creates an inference session | Browser Responses/realtime and AI Studio relay; accepted-prefix cursor and duplicate suppression | §6.7 |

Excluded from inference conversion: health/root/dashboard/management, model discovery (`/v1/models` and `/v1beta/models` GET families), OAuth callbacks, credential authentication, and ephemeral-key issuance (`POST /v1/realtime/client_secrets`, `/v1/realtime/sessions`). Transport handshakes remain boundary work, but their SDP and inference content are not exempt.

Existing 501 operations remain 501: POST `/v1/realtime/transcription_sessions`; GET/POST `/v1/realtime/translations`; POST `/v1/realtime/translations/client_secrets`; POST `/v1/realtime/calls/{call_id}/{accept,reject,refer}`. Unknown Gemini action strings are not implemented inference operations. Do not add providers, SIP, transcription or translation features to fill a matrix cell.

## Provider and delivery matrix

The sole selector is `crates/ck-exec/src/lib.rs:164-184`; image/video/speech selectors are at `:187-233`, session selection at `:236-260`. Provider aliases below are current dispatch inputs, not new compatibility aliases. “Buffered + stream” describes client delivery; even a buffered client may consume an upstream stream internally. Test local upstreams only.

| Provider selector | Wire boundary and generate modes | Count mode | Compact / special modes | Evidence in `crates/ck-exec/src/` |
| --- | --- | --- | --- | --- |
| `claude` | Messages; API-key and OAuth preparation; native and cross-format; buffered + stream | Local estimate or delegated upstream count | Summary compaction; bound signatures/redaction/cache and tool aliases | `claude.rs::execute_delegated`, `run`, `compact`, `count_tokens` |
| `codex` | Responses HTTP/SSE and WS; buffered + stream; continuation | Local shaped-request estimate | Native compact, image tools, alpha search, Live/Realtime | `codex.rs::execute`, `codex_duplex.rs`, `codex_live.rs` |
| `kimi`, `kimi-ai`, `kimi.ai`, `kimi.com` | Claude delegation for Claude clients; Chat and Responses branches for others; buffered + stream | Upstream Claude count | Responses compact returns 501; streaming compact rejected | `kimi.rs:122-159,754-762` |
| `meta` | Responses envelope; buffered collection or streamed events | Local token estimate | Compact returns 501 | `meta.rs:184-191`, `count_tokens` |
| `devin` | Interactions inside Connect/protobuf frames, including gzip/trailers; buffered + stream | Local body-length estimate | No compact-specific implementation; do not count ordinary generation under a compact alt as supported compact | `devin.rs::prepare_request`, `execute` |
| `openai-compatibility`, `openai-compatible-*` | Chat; buffered + stream | Local translated-Chat estimate | Nonstream Responses compact; images JSON/multipart and buffered/SSE output | `openai_compat.rs:35-39,390-395`, `count_tokens` |
| `gemini`, `gemini-interactions` | Gemini generation or Interactions; buffered + stream | Upstream count | Compact returns 501 | `gemini.rs::execute`, `count_tokens` |
| `vertex` | Vertex Gemini envelope; buffered + stream | Upstream count | Compact returns 501 | `vertex.rs::execute`, `handles` |
| `aistudio` | Gemini over registered browser relay; buffered + stream | Relay count | Compact returns 501; relay JSON/binary reconnect | `aistudio.rs::execute`, `count_tokens`; `wsrelay.rs` |
| `antigravity` | Wrapped Gemini response/request, replay and provider preparation; buffered + stream | Upstream count | Summary compaction; reject streamed compact route | `antigravity.rs::execute`, `compact`, `count_tokens` |
| `xai` | Responses HTTP and WS sessions; buffered + stream | Local shaped-request estimate | Native compact, compaction-trigger stream, images, video and raw speech | `xai.rs::execute`, `compact`, `speech`; `xai_ws.rs` |

For each generate row, cover the five public request families (Chat, Messages, Responses, Gemini, Interactions) wherever current routing and capability policy admit them, plus legacy through Chat. The 30-arm table provides exact recorded pairs; it does not grant unsupported capabilities. Preserve each mapped refusal and record its input and zero-send assertion. Count endpoints use the provider's existing count implementation, not generation billed as count. Media selectors admit only their listed providers; no missing media executor becomes an implementation task.

Responses WS uses retained sessions on Codex/xAI. Other executors use ordinary execution where supported; continuation requiring a retained upstream session returns the existing replay-required error rather than inventing a session. Test both HTTP and WS upstream choices independently of the client's protocol. WebRTC without `media-relay` must fail visibly; with it, all media must pass through codecs, including configurations that previously negotiated direct media.

## Semantic acceptance obligations

Every applicable route/provider/mode cell remains pending until its implementation records consumer-visible evidence. Use the fixture IDs above and the following loss IDs to locate cases; do not mark an entire family passed from one representative round-trip.

| Loss IDs | Replacement assertion | Required dimensions |
| --- | --- | --- |
| L01, L04, L06 | Preserve block-bound signatures, redaction/encryption, signature-only parts and native item identity | Request history and response; all four codec roles; native and cross-protocol |
| L02, L09, L18 | Preserve absent/null/zero, TTL buckets, corrected snapshots and terminal finishes; account once per attempt | Stream/buffered/count, realtime and resume; failed attempts |
| L03 | Emit tool start/fragments/end incrementally without invented arguments | Fragmented SSE, custom text, structured history and multiple calls/choices |
| L05, L14 | Keep unknown events, grounding/code/context and non-function lifecycle through egress | Destination emission or content-free B9 drop; nested unknown fields |
| L07, L08 | Keep every part in order; choose index 0, not array element 0, at single-choice egress | Interleaved choices, multiple same-kind parts and empty strings |
| L10, L11, L12, L16 | Preserve audio modalities, independent sampling, legacy fields and speech instructions | Requests; late capability refusal before any upstream send |
| L13 | Preserve logprobs-only events without fabricated text | Stream and buffered legacy/Chat projections |
| L15 | Keep image creation time, revised prompt and per-image identity | Native, emulated stream and buffered image results |
| L17 | Preserve native expired/cancelled/failed status and job identity | Create/retrieve/edit/extend and binary content |

Storage, flow and security acceptance are independent of fixture fidelity: complete-prefix publication, generation/lease safety, cross-frame parsing, metadata/frame limits, 100 ms collection and 50,000-byte encoded data records, actual slow downstream, pre-delivery retry, post-commit failure, disconnect/resume, no duplicate usage, last-read expiry/LRU and shutdown. Resume tests use the exact API/cursors in INTERNAL-STREAM §6.7 and inspect access/application logs, capture/spools and client storage for ID leakage.

## Old baseline build provenance

Captured on 2026-11-14 from post-W1 `b518060ea1e43ef4b266591986ca92321e6289b5`. The build used isolated checkout `/scratch/ssd/s1a-old`, `CARGO_TARGET_DIR=/scratch/ssd/s1a-old-target` and `TMPDIR=/scratch/ssd/s1a-old-target/tmp`; the checkout and target were removed after capture. The retained executables and `fix-baseline-*` provenance records are in the maintainer-held directory `/home/zakk/code/jev-router/p4/s1-baseline/`, outside the repository, not in a public artifact store. Later builds must not overwrite them.

| Artifact in that directory | SHA-256 | Size |
| --- | --- | ---: |
| `cooker-b518060e` | `4f89bd8e13abd3078adea569b7c61805b2a2f5d366219e4ae24883b28ea7a9fc` | 41,747,480 bytes |
| `claude_latency-b518060e` | `9f5fecbebb3b411f60292a2d2b72a3aeaf9a88e521ac3d9add4f76a564cc0d79` | 34,443,032 bytes |

| Field | Capture environment |
| --- | --- |
| Compiler | `rustc 1.99.0 (b940084d7 2026-09-28)`, full compiler commit `b940084d7eb6a299eb4bfeb8e34901bc051e7ac4`, LLVM 23.1.1 |
| Cargo / target | Cargo 1.99.0 (`5f94df478 2026-08-27`); `x86_64-unknown-linux-gnu` |
| Profile | `--locked --release -j 8`; workspace release profile: thin LTO, one codegen unit, `strip = true` |
| Features / flags | No explicit feature flags or custom Rust/compiler/linker environment flags; Cargo fingerprints record no `cooker` or `ck-server` features for the gateway, and `ck-server/testing` for the latency example through its dev self-dependency. `media-relay` and `pprof` are disabled in both. |
| Embedded dashboard | `npm ci --no-audit --no-fund` then `npm run build` in the old checkout's `ui/`, because `ui/dist` was absent; Node v26.13.1, npm 11.20.0 |
| CPU / affinity | AMD Ryzen 9 9950X3D 16-Core Processor; x86_64, 16 cores / 32 threads; inherited affinity CPUs 0-31 |
| Kernel / page size | Linux `7.3.4-gentoo-cjk-zakk`, `#1 SMP PREEMPT_DYNAMIC Fri Nov 13 15:40:29 AEDT 2026`; 4,096-byte pages |
| Build filesystem | `/scratch/ssd` on ZFS dataset `vmpool/scratch` |
| Host load at capture start/end | 1/5/15-minute averages: 1.77 / 1.92 / 2.26 at 2026-11-14 08:22:14 +11:00; 19.65 / 13.26 / 6.71 at 08:26:42 +11:00 after the builds. Full records: `fix-baseline-environment.log`. These are build-host loads, not benchmark results. |

Reproduction commands, from the isolated old checkout with the target and temporary directories above:

```sh
cargo build --manifest-path /scratch/ssd/s1a-old/Cargo.toml --locked --release -j 8 -p cooker --bin cooker
cargo build --manifest-path /scratch/ssd/s1a-old/Cargo.toml --locked --release -j 8 -p ck-server --example claude_latency
```

The pinned `ck-translate` package has library and test targets, but no standalone codec benchmark executable (`crates/ck-translate/Cargo.toml`; captured Cargo target inventory). Phase C's codec comparison interface must exercise equivalent fixtures through the pinned old pair API and the new boundary roles; neither the gateway nor the latency executable is a substitute for that codec harness.

Phase A checks validate documentation and the current dependency table only. Codec/pipeline tolerances remain PROPOSED in INTERNAL-STREAM §9.2 and require owner approval. Both old executable builds completed in the release profile; the copied gateway reports `cooker 0.4.1` with `--version`. No old-binary benchmark, S1 runtime gate or storage regression gate has run as part of this ledger freeze.
