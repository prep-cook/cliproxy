# REQUIREMENTS.md: numbers, gates and exceptions

This file decides cooker's commands, numbers, gates and exceptions. Rules live in `AGENTS.md`, `.github/AGENTS.md`, `ui/AGENTS.md` and `crates/AGENTS.md`.
The checker reads files and runs no gates. Write commands in backticks and escape cell-internal pipes as `\|`.

## Rule files

List every project `AGENTS.md`. Checked files supply `###` groups and tagged rules; unchecked files need a reason. The checker skips `.git`, `node_modules`, `vendor`, `third_party` and hidden directories other than `.github`.

| File | Checked | Note |
| --- | --- | --- |
| AGENTS.md | yes | Repository-wide rules; CLAUDE.md imports this file |
| .github/AGENTS.md | yes | CI rules; .github/CLAUDE.md imports this file |
| crates/AGENTS.md | yes | Backend rules; crates/CLAUDE.md imports this file |
| ui/AGENTS.md | yes | Frontend rules and unchanged Astryx block; ui/CLAUDE.md imports this file |

## Commands

One row per tier. Run commands with the setup and isolation defined in the named workflows. Set PR_TITLE to the proposed PR title before the title check. The requirements check and its self-tests are the `requirements` stage, which the `requirements` job builds on every run and the `check` stage includes, so their failure fails `ci-ok`.
Measure three runs on the build host with date and load before setting a whole-tier limit with margin. Existing workflow timeouts and dashboard limits remain in force.

| Tier | Command | Runs (where, when) | Wall-time limit | Baseline (date, host, load) |
| --- | --- | --- | --- | --- |
| Pre-commit | | No tier exists yet | | |
| Merge gate | `docker buildx build --target dashboard .`; `docker buildx build --target check .`; `docker buildx build --target test-report --build-arg SHARD=1 --build-arg SHARDS=2 --output type=local,dest=out1 .` and the same with `SHARD=2`, then `python3 .github/scripts/test-counts.py total out1/counts-1.json out2/counts-2.json`; `docker buildx build --target perf .`; `docker buildx build --target runtime --load -t cooker:ci .`; `docker run --rm cooker:ci --version`; `bench/gate.sh image cooker:ci`; `python3 -I tools/changelog.py check`; `python3 -I .github/scripts/pr-title.py "$PR_TITLE"`; `python3 -m unittest discover -s .github/scripts -p 'test_ci_lanes*.py'`; `docker buildx build --target requirements .` | ci.yml selects lanes on PRs; all lanes on master, manual/release calls and ci:full reruns. The stage contents are in `Dockerfile`. Changes workflow runs fragments/title. The requirements stage (the check and its self-tests) runs on every run, including rule-only and documentation-only changes, and contributes to the `ci-ok` result. | | |
| Release | `git push origin vX.Y.Z`; after approval, `gh workflow run release.yml -f tag=vX.Y.Z` | The tag runs every CI lane, builds `runtime` natively for amd64 and arm64, checks the tag against `--version` and the image size, pushes the `candidate-X.Y.Z-<commit>` images and creates a draft release recording the candidate's digest. The manual run checks that the tag still points at that commit and that the image reports `X.Y.Z`, tags that digest `X.Y.Z` and `latest` and publishes the draft. Publishing follows root Rule priority. | | |

Before opening a PR, run on the final tree:

- `docker buildx build --target check .`
- `docker buildx build --target test .`
- `docker buildx build --target dashboard .`
- `docker buildx build --target requirements .` when only rule or documentation files changed; the `check` stage includes it otherwise.
- After changes to anything running while the server is idle, also run `docker buildx build --target perf .`.

Before every release, run the dashboard performance harness using `ui/tools/perf/README.md` and compare its measurements with the recorded baseline. This is a measurement step, not a merge gate; exit 0 proves completion, not acceptable performance.

Stage commands and isolation are defined in `Dockerfile`; lanes and coverage in `.github/workflows/ci.yml` and `.github/AGENTS.md`.

## Numbers

Keep authoritative datasets in their existing source files. Blank baselines await build-host measurement.
UI scale/layout values remain in `ui/DESIGN.md` and code limits in `ui/CONVENTIONS.md`; the empty template rows track their measurement/adoption rather than duplicating those values.

| Item | Limit | Baseline (date, host, load) | Enforced by |
| --- | --- | --- | --- |
| Type scale (role: size / line height) | | | ui/DESIGN.md; design system |
| Spacing scale (section, card, inside card, label to field) | | | ui/DESIGN.md; design system |
| Page frame width; readable column width | | | ui/DESIGN.md; rendered measurement |
| Supported widths (phone, laptop, wide) | | | ui/DESIGN.md; screenshots |
| Full end-to-end run, wall time | 60 s per test, 5 min total; before merge, not per commit | | Review browser-check logs |
| Slowest test package | | | Build-host measurement pending |
| Frontend entry chunk, gzip | Initial JS 360000 B; initial CSS 55000 B; largest lazy chunk 120000 B; source: ui/tools/size-budget.json | 333632 B, 47776 B, 105870 B (2026-11-09, committed build; host/load not recorded) | npm run check in ui/; ui/tools/size.mjs |
| Frontend total JS + CSS, gzip | Report-only, no ceiling; ui/tools/size-budget.json totalJs/totalCss are null | JS 946229 B, CSS 55811 B (2026-11-09, committed build; host/load not recorded) | ui/tools/size.mjs |
| Source file size, p90 of this repository (pass bar) | | | Measure physical lines in crates/*/src/**/*.rs, ui/src/**/*.ts and ui/src/**/*.tsx; exclude tests/, testdata/, fixtures/, generated files and *.test.*, *.spec.*; include inline Rust tests; review |
| Wait timeout before a stuck step is recorded and left | | | Agent; decision 13 |
| Context handoff limit (share of the model's window) | | | Orchestrator; decision 13 |
| Status check interval while several jobs run | | | Orchestrator; decision 13 |
| Commit subject length | 72 characters; source: CONTRIBUTING.md, Commits and pull request titles | A setting, not measured | .github/scripts/pr-title.py checks PR titles; commit hook absent |
| Linux release sizes | Binary 52000000 B; text 35000000 B; rodata 11000000 B; zero tolerance; source: bench/budgets.txt size.x86_64-unknown-linux-gnu | | bench/gate.sh size |
| Duplicate linked crate versions | 11 crates, zero tolerance; source: bench/budgets.txt deps.x86_64-unknown-linux-gnu.duplicate_crates | | bench/gate.sh size |
| Linux idle | With 1 and 2000 credentials: 3 wakeups, 1 CPU tick, 6 threads; RSS 33500 and 85800 KB with 10% tolerance, measured in the Dockerfile perf stage; source: bench/budgets.txt idle.linux | | bench/gate.sh idle |
| Field-mix soak peak | 173072 KB with 20% tolerance; source: bench/budgets.txt soak.field.peak_hwm_kb | | bench/soak.sh |
| Request heap budgets | Per-route peak bytes, allocations and held bytes with keyed tolerances in bench/budgets.txt alloc.* | | crates/ck-server/tests/alloc_budget.rs through nextest |
| Dashboard npm run check wall time | 90 s local, 5 min CI | 5.8 s (2026-11-08, shell and System page; host/load not recorded) | ui/tools/check-all.mjs enforces 90 s, including in CI. The 5 min CI figure is the recorded target; the dashboard job's workflow timeout is 30 min (`.github/workflows/ci.yml`) |
| Accounts, 1000 accounts: load to all rows painted | ≤ baseline | | ui/tools/perf/run.mjs; before-release comparison |
| One 10 s poll on Accounts, main thread | ≤ baseline | | ui/tools/perf/run.mjs; before-release comparison |
| JS heap after 5 min of polling | ≤ baseline | | ui/tools/perf/run.mjs; before-release comparison |
| Idle CPU on Accounts | ≤ baseline | | ui/tools/perf/run.mjs; before-release comparison |
| Select all, main thread | ≤ baseline | | ui/tools/perf/run.mjs; before-release comparison |
| Contrast, project targets | Body ≥ 10:1; secondary and status text ≥ 5:1 | Moon dark: body 11.86, lowest 3.94; Main dark: body 13.39, lowest 3.16; Dawn light, both palettes: body 8.69, lowest 2.86 (browser, 1440 px, en; host/date not recorded); Exceptions lists misses | Browser measurement, by hand |
| Contrast, WCAG AA | Normal text 4.5:1; large text, UI components and focus indicators 3:1 | Neutral and optional-palette measurements under Exceptions; optional palettes have an owner exception | Browser measurement, by hand |
| Dashboard session lifetime | Default session 7 days; remember-this-device 30 days | Owner setting, 2026-11-13 | `SHORT` and `LONG` in crates/ck-server/src/management/api/session.rs; `dashboard_session_cookie_signs_in_reads_and_signs_out` in crates/ck-server/tests/management.rs |

Historical source evidence: `bench/budgets.txt` records dated compiler/runner measurements and tolerances; `ui/REQUIREMENTS.md` retains historical dashboard measurements. Dates, hosts and load are stated only where recorded.

## Deploy-only stream settings

S1 fixes this configuration contract; `ck-stream::Config` implements it for the store, while production bootstrap integration remains part of the S1 execution cutover. All seven values are read and validated at startup. Every change requires a restart. Configured overrides remain supported within the validation and owner ceilings in `docs/INTERNAL-STREAM.md`, §6.4. Schema declarations must mark these values deploy-only; runtime configuration publication must not hot-apply them.

| Setting | Classification | Contract reference |
| --- | --- | --- |
| `stream.pool_directory` | Deploy-only; restart required | Private directory and file lifecycle, §6.3 |
| `stream.pool_bytes` | Deploy-only; restart required | Startup tmpfs/cgroup caps and total ceiling, §6.4 |
| `stream.startup_reserve_bytes` | Deploy-only; restart required | Prebacked warm frames within the pool cap, preserved by decay, §6.4–6.5 |
| `stream.max_frames` | Deploy-only; restart required | Derived capped-pool default and combined exchange quota, §6.4 |
| `stream.max_event_bytes` | Deploy-only; restart required | Provisional event limit and startup validation, §6.4 |
| `stream.max_metadata_bytes` | Deploy-only; restart required | Metadata limit (8 MiB default, owner number) and checked capacity validation, §6.4 |
| `stream.cache_ttl` | Deploy-only; restart required | Last-read expiry, §6.4 |

These values need no live-editable admin page or page exception under `crates/AGENTS.md`, Admin panel. Later hot-change support must include its admin page in the same change. Owner-approved codec/pipeline tolerances in `docs/INTERNAL-STREAM.md`, §9.2, allow at most 5% regression per metric and 0% for memory/storage (2026-11-14 morning, option B); they do not change the existing budgets above or the §9.3 storage gate.

## Enforcement

Names exactly match `###` headings and tags. Gate commands cover existing mechanical checks; Review criterion names the exact `REVIEW.md` section for judgment. Missing required gates remain blank and dated under "Not yet enforced".

| Rule group | Gate command | Review criterion | Does not apply because |
| --- | --- | --- | --- |
| AGENTS.md: Change risk | | `REVIEW.md`, "Scope" | |
| AGENTS.md: Workflow | | `REVIEW.md`, "Scope", "Pass bar" and "Report" | |
| AGENTS.md: When unsure | | `REVIEW.md`, "Rule priority" and "Scope" | |
| AGENTS.md: Waiting | | `REVIEW.md`, "Every finding has evidence"; check execution records against the configured settings | |
| AGENTS.md: Reporting | | `REVIEW.md`, "Every finding has evidence" and "Report" | |
| AGENTS.md: Code quality | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`; `npm run check` in ui/ | `REVIEW.md`, Pass bar; checks cover formatting/lints, not all design judgment | |
| AGENTS.md: Impact and edge cases | | `REVIEW.md`, "Scope" and "Checking against the rules" | |
| AGENTS.md: Secrets | | `REVIEW.md`, "Checking against the rules" and "Review guidelines" | |
| AGENTS.md: Performance baseline | | `REVIEW.md`, "Every finding has evidence"; compare same-host measurements | |
| AGENTS.md: Tests (if present) | `cargo nextest run --workspace --locked`; `npm run check` in ui/ | `REVIEW.md`, Every finding has evidence; failing-without-fix and ablation proof remain manual | |
| AGENTS.md: Merging | | `REVIEW.md`, "Before you start" and "CI, docs and commits"; check required statuses, review and approvals | |
| AGENTS.md: Model usage | | `REVIEW.md`, "Before you start" | |
| AGENTS.md: Documentation | `python3 -I tools/changelog.py check`; `python3 -I tools/third-party-notices.py --check` | `REVIEW.md`, CI, docs and commits; prose and all generated files need review | |
| AGENTS.md: Writing (docs, comments, reports, PR and issue text) | | `REVIEW.md`, "Pass bar" and "CI, docs and commits" | |
| AGENTS.md: Commits and pull requests | `python3 -I .github/scripts/pr-title.py "$PR_TITLE"` | `REVIEW.md`, CI, docs and commits; signing/body/scope remain review | |
| AGENTS.md: Frontend–backend mapping (when both exist) | `node ui/tools/check-api.mjs` | `REVIEW.md`, Checking against the rules; error notification mapping and origin boundary remain review | |
| .github/AGENTS.md: Tiers, jobs and gates | `python3 -m unittest discover -s .github/scripts -p 'test_ci_lanes*.py'`; `actionlint` | `REVIEW.md`, "CI, docs and commits" | |
| ui/AGENTS.md: Components and modularity | `npm run check` in ui/ | `REVIEW.md`, Pass bar and Checking against the rules | |
| ui/AGENTS.md: Consistency | `npm run check` in ui/ | `REVIEW.md`, Checking against the rules; visual consistency remains review | |
| ui/AGENTS.md: Layout, type and loading | | `REVIEW.md`, "Checking against the rules"; measure real-data screenshots | |
| ui/AGENTS.md: i18n (if present) | `npm run check` in ui/ | `REVIEW.md`, Checking against the rules, Copy/i18n | |
| ui/AGENTS.md: Usability | | `REVIEW.md`, "Checking against the rules"; inspect flow precedents and supported surfaces | |
| ui/AGENTS.md: Accessibility | | | |
| ui/AGENTS.md: Performance | `npm run check` in ui/ | `REVIEW.md`, Checking against the rules; bundle ceilings gated, runtime baselines pending | |
| ui/AGENTS.md: Dashboard sessions | `cargo nextest run --workspace --locked`; `npm run check` in ui/ | `REVIEW.md`, Checking against the rules, Authorization and Sensitive data; a real-browser sign-in, reload and sign-out remain manual | |
| ui/AGENTS.md: API error notifications | | | |
| crates/AGENTS.md: APIs and modularity | `cargo clippy --workspace --all-targets --locked -- -D warnings` | `REVIEW.md`, Review guidelines; Go case-method bans gated, passthrough and boundaries reviewed | |
| crates/AGENTS.md: Compatibility | | `REVIEW.md`, "Checking against the rules" and "Review guidelines" | |
| crates/AGENTS.md: Logging | | `REVIEW.md`, "Review guidelines" | |
| crates/AGENTS.md: Security (if present) | | `REVIEW.md`, "Checking against the rules" | |
| crates/AGENTS.md#authz | | | |
| crates/AGENTS.md#sensitive-values | | | |
| crates/AGENTS.md#management-cross-site | | | |
| crates/AGENTS.md: Money and state (if present) | `cargo nextest run --workspace --locked` | `REVIEW.md`, Checking against the rules; state invariants in docs/STATE-AND-CONCURRENCY.md, conditional money clauses pending decision 8 | |
| crates/AGENTS.md#money-idempotent | | | |
| crates/AGENTS.md#ledger | | | |
| crates/AGENTS.md: Background work (if present) | `bench/gate.sh idle linux "${CARGO_TARGET_DIR:-target}/release/cooker" 1` | `REVIEW.md`, Review guidelines; lifecycle/shutdown proof remains review | |
| crates/AGENTS.md: Operations (if present) | | `REVIEW.md`, "Checking against the rules" and "CI, docs and commits"; inspect approval and restore evidence | |
| crates/AGENTS.md: Admin panel (if present) | | | |
| crates/AGENTS.md: Performance | `bench/gate.sh size x86_64-unknown-linux-gnu "${CARGO_TARGET_DIR:-target}/release/cooker"`; `cargo nextest run --workspace --locked` | `REVIEW.md`, Review guidelines; linear preparation and prompt-cache live proof remain review | |
| crates/AGENTS.md: Verification | `cargo nextest run --workspace --locked` | `REVIEW.md`, Every finding has evidence and Review guidelines; isolation follows ci.yml setup | |
| crates/AGENTS.md: Cargo workspace layout | `python3 -I tools/check-crate-deps.py` | `REVIEW.md`, Review guidelines; naming, test placement and `lib.rs` headers reviewed | |
| crates/AGENTS.md: Management API contract | | `REVIEW.md`, "Checking against the rules"; mechanical coverage under Management response schemas | |
| crates/AGENTS.md: OpenAPI drift | | | |
| crates/AGENTS.md: Management response schemas | | | |
| crates/AGENTS.md: Management API version | | | |
| crates/AGENTS.md: Management error catalog | | | |
| crates/AGENTS.md: Dashboard artifact integrity | | | |

## Exceptions

Backend endpoints with no frontend caller:

| Endpoint | Caller | Reason |
| --- | --- | --- |
| Rows in ui/tools/api-allowlist.json | Per-row CLI, provider callback, other admin client or equivalent dashboard operation | The authoritative method/path/reason list consumed by check-api; migrate its entries with their routes. |

Intentional designs that look like defects:

| What | Why | Owner ruling (date, quote) |
| --- | --- | --- |
| Compatibility: cooker keeps no backward compatibility in any surface (config, database, CLI, environment, routes, responses, paths, crate names); the management API starts at EXPERIMENTAL `/admin/v0` | It is a full rewrite of an existing project. Valid until an owner sets a compatibility policy, for example at v1. | An owner, 2026-11-13: 「API 要不直接从 v0 开始计，不要向后兼容」; 「完全不需要考虑任何向后兼容性，我们是基于一个现有项目的完整重写」 |
| Dashboard shows provider status/message and OAuth failure detail | Dashboard users operate the upstream connection; backend Security permits operator detail | Existing ui/REQUIREMENTS.md design; no dated owner quote supplied |
| Sign-in has one raw hidden input carrying the host as username | Password managers need a username; Astryx VisuallyHidden is a span and must not wrap a control | Existing ui/REQUIREMENTS.md design; no dated owner quote supplied |
| Live usage is opt-in and tab-local | Reading the queue consumes records other consumers could read | Existing ui/REQUIREMENTS.md design; preserve semantics during migration |
| Write actions are probed before being enabled; unsupported writes are disabled | OPTIONS does not distinguish missing and supported routes | Existing ui/REQUIREMENTS.md design; retire with the version/contract cutover, not as an unrelated rules fix |
| Config writes reread and compare, refusing stale copies | Prevent two operators or external file edits overwriting one another | Existing ui/REQUIREMENTS.md rationale; not permission for bypass writes under the current state contract |
| Quota fetches are on request, sequential, at most once per account every two minutes | Each fetch is a real provider request | Existing ui/REQUIREMENTS.md design; ordinary tests remain mock-only |
| Batch actions affect the whole shared credential file and say so in the dialog | Server deletion/toggle operates on files | Existing ui/REQUIREMENTS.md design |
| Polling pauses in hidden tabs | Avoid work for an unseen dashboard | Existing ui/REQUIREMENTS.md design |
| ProviderIcon.tsx draws brand marks from vendored providerMarks.ts path data | Lucide lacks brand marks; monochrome vendored artwork follows text color; check:ui permits that file's SVG | Existing ui/REQUIREMENTS.md design; attribution/license provenance stays in the existing source and notices |
| Optional Rosé Pine palettes (Main, Moon, Dawn) may fall below WCAG AA | Preserve the optional palettes' published colors; the default Neutral palette must meet AA. Measurements below distinguish AA failures from stricter project-target misses. | An owner, 2026-11-13: 「主題的話是例外的」 |

Contrast inventory copied from `ui/REQUIREMENTS.md`. Browser rows cover rendered sign-in/System pages, open menus and info/error banners at 1440 px in English. Palette rows are computed mapped pairs, not proof they appear in a measured page. Dawn applies to both Rosé Pine palettes. Disabled text is exempt where noted; that does not exempt placeholder text.

Neutral light success (4.53:1) and info (4.85:1) on the page both pass WCAG AA. They miss only the stricter 5:1 project target in `ui/DESIGN.md`; neither is an exception to the AA rule.

Neutral's built palette measures body 18.59 light / 15.13 dark, secondary 8.23 / 5.65, warning text 10.27 / 8.65, and warning badges 7.70 / 6.46. Bar fills are 3.15 / 3.85 and chart volume 4.70 / 3.67 against the 3:1 non-text limit. Source: historical `ui/REQUIREMENTS.md`; date/host/load not recorded.

| Mode | Foreground on background | Ratio | Limit | Measured | Where |
| --- | --- | --- | --- | --- | --- |
| Moon dark | text on overlay | 8.82 | 10:1 | browser | Menu items |
| Moon dark | text on highlight med | 7.41 | 10:1 | browser | Selected nav row, neutral badge, Kbd, Go to page |
| Moon dark | text on overlay under hover | 7.65 | 10:1 | browser | Hovered/open menu |
| Moon dark | pine on surface | 3.94 | 5:1 | browser | Sign-in status |
| Moon dark | love on overlay | 4.00 | 5:1 | browser | Errors, destructive labels, log error level |
| Moon dark | subtle on surface | 4.46 | 5:1 | browser | Secondary text |
| Moon dark | subtle on base | 4.86 | 5:1 | browser | Secondary page/nav text |
| Main dark | pine on surface | 3.16 | 5:1 | browser | Sign-in status |
| Main dark | text on highlight med | 7.93 | 10:1 | browser | Selected nav row, neutral badge, Kbd, Go to page |
| Dawn light | text on base | 8.69 | 10:1 | browser | Body text |
| Dawn light | text on surface | 9.14 | 10:1 | browser | Body text on cards/content |
| Dawn light | text on overlay | 7.92 | 10:1 | browser | Menus |
| Dawn light | text on highlight med | 6.86 | 10:1 | browser | Selected nav row, neutral badge, Kbd, Go to page |
| Dawn light | text on overlay under hover | 7.31 | 10:1 | browser | Hovered/open menu |
| Dawn light | base on text | 8.69 | 10:1 | browser | Primary labels |
| Dawn light | foam on overlay | 2.86 | 5:1 | browser | Info banner |
| Dawn light | love on overlay | 3.50 | 5:1 | browser | Errors, destructive labels, log error level |
| Dawn light | gold on overlay | 1.87 | 5:1 | browser | Warning log level |
| Dawn light | iris on surface | 3.65 | 5:1 | browser | Skip link |
| Dawn light | subtle on base | 4.02 | 5:1 | browser | Secondary page/nav text |
| Dawn light | subtle on surface | 4.23 | 5:1 | browser | Secondary cards/content text |
| Moon dark | subtle on overlay | 3.61 | 5:1 | palette | Menus/popovers |
| Main dark | subtle on overlay | 4.71 | 5:1 | palette | Menus/popovers |
| Dawn light | subtle on overlay | 3.67 | 5:1 | palette | Menus/popovers |
| Moon dark | muted on base / surface / overlay | 3.03 / 2.79 / 2.25 | 5:1 | palette | Tertiary/placeholder text; disabled exempt |
| Main dark | muted on base / surface / overlay | 3.42 / 3.20 / 2.94 | 5:1 | palette | Tertiary/placeholder text; disabled exempt |
| Dawn light | muted on base / surface / overlay | 2.73 / 2.87 / 2.48 | 5:1 | palette | Tertiary/placeholder text; disabled exempt |
| Moon dark | pine on base / overlay | 4.29 / 3.19 | 5:1 | palette | Ready text |
| Main dark | pine on base / overlay | 3.38 / 2.91 | 5:1 | palette | Ready text |
| Moon dark | love on surface | 4.94 | 5:1 | palette | Failing text |
| Dawn light | love on base / surface | 3.84 / 4.04 | 5:1 | palette | Failing text |
| Dawn light | gold on base / surface / overlay | 2.05 / 2.16 / 1.87 | 5:1 | palette | Cooling text/warning banner |
| Dawn light | foam on base / surface | 3.14 / 3.30 | 5:1 | palette | Info text |
| Dawn light | iris on base / overlay | 3.47 / 3.16 | 5:1 | palette | Links |
| Neutral light | success text #2f7d33 on page #f1f1f1 | 4.53 | 5:1 | palette | Ready text; 5.12 on white surfaces |
| Neutral light | info text #0067ce on page #f1f1f1 | 4.85 | 5:1 | palette | Info text |

## Not yet enforced

Every incomplete Commands, Numbers or Enforcement row has one entry below. Due dates are calendar deadlines. Move a date later only with approval from an owner quoted in the commit body. Remove a deferral when its row is filled.

| Row | Current state | Plan | Due |
| --- | --- | --- | --- |
| Commands: Pre-commit | No hook/tier command | Add read-only changed-file checks; measure on build host before setting limit | 2026-12-08 |
| Commands: Merge gate | Lane commands and the requirements check run under `ci-ok`; tier baseline and limit unmeasured | Measure three same-host runs with load and set the limit | 2026-12-08 |
| Commands: Release | Tier baseline/limit and full release proof absent | Measure; add gate self-tests, reproducibility and deployment rehearsal | 2026-12-31 |
| Numbers: Type scale (role: size / line height) | Values live in ui/DESIGN.md; baseline unrecorded here | Measure rendered roles; reference existing values | 2026-12-08 |
| Numbers: Spacing scale (section, card, inside card, label to field) | Existing design tokens; baseline pending | Measure repeated relationships | 2026-12-08 |
| Numbers: Page frame width; readable column width | Existing frames; baseline pending | Measure changed page kinds at supported widths | 2026-12-08 |
| Numbers: Supported widths (phone, laptop, wide) | Existing design widths; complete baseline pending | Record screenshots and host/browser context | 2026-12-08 |
| Numbers: Full end-to-end run, wall time | No build-host baseline | Measure the complete browser suite on the build host without relaxing the existing limits | 2026-12-15 |
| Numbers: Slowest test package | No package-cost baseline | Measure package costs on build host | 2026-12-15 |
| Numbers: Source file size, p90 of this repository (pass bar) | No adopted baseline | Measure the source set defined in Numbers before using the pass bar | 2026-12-01 |
| Numbers: Wait timeout before a stuck step is recorded and left | Owner setting absent | Resolve decision 13 | 2026-12-01 |
| Numbers: Context handoff limit (share of the model's window) | Owner setting absent | Resolve decision 13 | 2026-12-01 |
| Numbers: Status check interval while several jobs run | Owner setting absent | Resolve decision 13 | 2026-12-01 |
| Numbers: Linux release sizes | Existing gated ceilings; complete baseline pending | Record date/host/load without raising limits | 2026-12-15 |
| Numbers: Duplicate linked crate versions | Existing gated ceiling; complete baseline pending | Record dependency set and build context | 2026-12-15 |
| Numbers: Linux idle | Existing gated values; full baseline context pending | Remeasure on build host; retain source tolerances | 2026-12-15 |
| Numbers: Field-mix soak peak | Existing gated value; full baseline context pending | Record same-host soak evidence | 2026-12-22 |
| Numbers: Request heap budgets | Existing keyed budgets; full baseline context pending | Record allocator workload and host/load | 2026-12-15 |
| Numbers: Accounts, 1000 accounts: load to all rows painted | Baseline absent | Measure the dashboard performance harness on build host | 2026-12-22 |
| Numbers: One 10 s poll on Accounts, main thread | Baseline absent | Measure the dashboard performance harness on build host | 2026-12-22 |
| Numbers: JS heap after 5 min of polling | Baseline absent | Measure the dashboard performance harness on build host | 2026-12-22 |
| Numbers: Idle CPU on Accounts | Baseline absent | Measure the dashboard performance harness on build host | 2026-12-22 |
| Numbers: Select all, main thread | Baseline absent | Measure the dashboard performance harness on build host | 2026-12-22 |
| Enforcement: ui/AGENTS.md: Accessibility | Contrast measured by hand; no automatic check | Decide whether contrast automation earns its cost; retain keyboard and contrast review under REVIEW.md, Checking against the rules, and honor palette exceptions | 2026-12-22 |
| Enforcement: ui/AGENTS.md: API error notifications | Levels defined in ui/DESIGN.md; API-error-to-notify mapping not tested | Add a unit test with the first write action whose errors reach notify() | 2026-12-15 |
| Enforcement: crates/AGENTS.md#authz | No actual-route-table authorization gate | Enumerate actual routes, reject routes neither authorized nor explicitly public, and plant a violation | 2026-12-15 |
| Enforcement: crates/AGENTS.md#sensitive-values | No complete free-text write/log gate | Implement a gate submitting planted sensitive values through each covered write path; assert refusal or masking and no leakage in application/database logs; wire into merge tier and prove a planted violation fails | 2026-12-15 |
| Enforcement: crates/AGENTS.md#management-cross-site | management/api/access.rs permits wildcard CORS and authenticates without the required Host/cross-site checks | Security PR: reject cross-site requests before key comparison; use a real-browser gate to prove Host validation, Origin/Sec-Fetch-Site rejection and no loopback ban | 2026-12-08 |
| Enforcement: crates/AGENTS.md#money-idempotent | No money operations; tagged exclusion needs approval | Resolve decision 8 and record quoted approval before marking inapplicable | 2026-12-01 |
| Enforcement: crates/AGENTS.md#ledger | No ledger; tagged exclusion needs approval | Resolve decision 8 and record quoted approval before marking inapplicable | 2026-12-01 |
| Enforcement: crates/AGENTS.md: Admin panel (if present) | Runtime-setting/page mapping is manual; API-only classification awaits decision 12 | Resolve decision 12; compare runtime settings with pages and ui/tools/api-allowlist.json in a merge gate; plant an unmapped setting; meanwhile review under REVIEW.md, Checking against the rules | 2026-12-22 |
| Enforcement: crates/AGENTS.md: OpenAPI drift | Rust-generated schema and dashboard type drift gate absent | Generate both from Rust types, regenerate in CI and reject differences | 2026-12-22 |
| Enforcement: crates/AGENTS.md: Management response schemas | Actual-response schema/header/example coverage gate absent | Cover every operation and response status, envelopes/links, filters, authority-specific preconditions and atomic failures, request ids and removed routes including fallbacks; plant field-shape and required-header violations | 2026-12-22 |
| Enforcement: crates/AGENTS.md: Management API version | Version/fragment gate absent | Check schema changes increment the version and add a fragment; prove bidirectional headers and mismatch rejection before mutation | 2026-12-22 |
| Enforcement: crates/AGENTS.md: Management error catalog | Stable-code/shared-schema gate absent | Reject unregistered or renamed codes and errors outside the common schema | 2026-12-22 |
| Enforcement: crates/AGENTS.md: Dashboard artifact integrity | Builds embed the Dockerfile dashboard stage's ui/dist; per-file manifest gate absent | Artifact PR: allow backend-only development, require real assets for shipping, and prove missing assets or a checksum mismatch fail | 2026-12-08 |
