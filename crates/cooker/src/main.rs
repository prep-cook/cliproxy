mod config_cli;
mod discovery;
mod dotenv;
mod tui;

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ck_core::config::Config;
use ck_exec::Executors;
use ck_exec::claude::{ClaudeExecutor, DEFAULT_BASE_URL};
use ck_server::Runtime;
use ck_server::net::listener::bind;
use ck_server::store::config_store::ConfigStore;
use ck_server::store::credential_store::CredentialDb;
use clap::{ArgAction, Parser};

/// Same strings as the management `X-Ck-*` headers.
const VERSION: &str = concat!("cooker-", env!("CARGO_PKG_VERSION"));
const COMMIT: &str = match option_env!("CK_COMMIT") {
    Some(v) => v,
    None => "none",
};
const BUILD_DATE: &str = match option_env!("CK_BUILD_DATE") {
    Some(v) => v,
    None => "unknown",
};

/// The server's command-line flags.
#[derive(Clone, Parser)]
#[command(
    version,
    about,
    after_help = "Config commands: config import FILE | config export [--secrets] [-o FILE] | config validate [FILE] | config backup FILE | config listener\nUse config --help for details."
)]
struct Args {
    /// The config file a first start imports into the config database (default
    /// ./config.yaml; without that file the first start uses defaults and port 8317);
    /// later starts do not read it.
    #[arg(long, default_value = "")]
    config: String,
    /// The config database (default: cooker.db beside the config file, or CK_DB).
    #[arg(long, default_value = "")]
    db: String,
    /// Write process logs to this file (10 MiB rotation), without a shell wrapper.
    #[arg(long)]
    log_file: Option<PathBuf>,
    /// Change to this directory first, before reading .env or relative paths.
    #[arg(long)]
    working_dir: Option<PathBuf>,
    /// Log in to Claude using browser OAuth and PKCE.
    #[arg(long)]
    claude_login: bool,
    /// Login to Codex using OAuth
    #[arg(long)]
    codex_login: bool,
    /// Login to Codex using device code flow
    #[arg(long)]
    codex_device_login: bool,
    /// Don't open browser automatically for OAuth
    #[arg(long)]
    no_browser: bool,
    /// Override OAuth callback port (defaults to provider-specific port)
    #[arg(long, default_value_t = 0)]
    oauth_callback_port: u16,
    /// Login to Antigravity using OAuth
    #[arg(long)]
    antigravity_login: bool,
    /// Login to Kimi (.com) using OAuth
    #[arg(long)]
    kimi_login: bool,
    /// Login to Kimi.ai using OAuth
    #[arg(long)]
    kimi_ai_login: bool,
    /// Login to xAI using OAuth
    #[arg(long)]
    xai_login: bool,
    /// Login to Devin using OAuth
    #[arg(long)]
    devin_login: bool,
    /// Login to Meta using OAuth
    #[arg(long)]
    meta_login: bool,
    /// Discover local AI gateways and cooker instances on the LAN
    #[arg(long)]
    discover: bool,
    /// Timeout in seconds for LAN discovery (default 3s)
    #[arg(long, default_value_t = 3)]
    discover_timeout: i64,
    /// Output discovered gateways in JSON format
    #[arg(long)]
    discover_json: bool,
    /// DNS-SD service type for LAN discovery (default _ai-gateway._tcp)
    #[arg(long, default_value = "")]
    discover_service_type: String,
    /// Comma-separated interface names to scan during LAN discovery
    #[arg(long, action = ArgAction::Append)]
    discover_include: Vec<String>,
    /// Comma-separated interface names to skip during LAN discovery
    #[arg(long, action = ArgAction::Append)]
    discover_exclude: Vec<String>,
    /// Import Vertex service account key JSON file
    #[arg(long, default_value = "")]
    vertex_import: String,
    /// Prefix for Vertex model namespacing (use with --vertex-import)
    #[arg(long, default_value = "")]
    vertex_import_prefix: String,
    /// Bind Sign in with ChatGPT (SIWC) credentials to a Codex credential file
    #[arg(long, default_value = "")]
    siwc_bind: String,
    /// Management password accepted from loopback clients only.
    #[arg(long, hide = true, default_value = "")]
    password: String,
    /// Start with terminal management UI
    #[arg(long)]
    tui: bool,
    /// In TUI mode, start an embedded local server
    #[arg(long)]
    standalone: bool,
    /// Base URL of remote management API for TUI/config clients (e.g. https://proxy.example.com)
    #[arg(long, default_value = "")]
    management_base_url: String,
    /// Use embedded models.json and codex_client_models.json only, skip remote model catalog fetching
    #[arg(long)]
    local_model: bool,
}

/// Go's `discover` subcommand flag set.
#[derive(Parser)]
#[command(name = "discover")]
struct DiscoverArgs {
    /// Discovery timeout in seconds
    #[arg(long, default_value_t = 3)]
    timeout: i64,
    /// Output in JSON format
    #[arg(long)]
    json: bool,
    /// DNS-SD service type (default _ai-gateway._tcp)
    #[arg(long, default_value = "")]
    service_type: String,
    /// Configure File Path
    #[arg(long, default_value = "")]
    config: String,
    /// Comma-separated interface names to scan (overrides default physical LAN filter)
    #[arg(long, action = ArgAction::Append)]
    include: Vec<String>,
    /// Comma-separated interface names to skip
    #[arg(long, action = ArgAction::Append)]
    exclude: Vec<String>,
}

/// The environment variable an online command reads its management key from.
const MANAGEMENT_KEY_ENV: &str = "CK_MANAGEMENT_KEY";

/// The management key for an online command: `--password` when given, else
/// `CK_MANAGEMENT_KEY`, else a prompt that does not echo on an interactive
/// terminal. Non-interactive callers without either send no key, as before.
pub(crate) fn management_key(explicit: &str) -> anyhow::Result<String> {
    use std::io::IsTerminal as _;
    resolve_management_key(
        explicit,
        std::env::var_os(MANAGEMENT_KEY_ENV),
        io::stdin().is_terminal() && io::stderr().is_terminal(),
        prompt_management_key,
    )
}

fn resolve_management_key(
    explicit: &str,
    env: Option<std::ffi::OsString>,
    interactive: bool,
    prompt: impl FnOnce() -> anyhow::Result<String>,
) -> anyhow::Result<String> {
    if !explicit.is_empty() {
        return Ok(explicit.to_owned());
    }
    if let Some(env) = env.filter(|value| !value.is_empty()) {
        return env
            .into_string()
            .map_err(|_| anyhow::anyhow!("{MANAGEMENT_KEY_ENV} is not valid UTF-8"));
    }
    if interactive { prompt() } else { Ok(String::new()) }
}

async fn online_management_key(explicit: String) -> anyhow::Result<String> {
    tokio::task::spawn_blocking(move || management_key(&explicit))
        .await
        .context("read the management key")?
}

/// Reads one line from the terminal in raw mode, so the key is never echoed.
fn prompt_management_key() -> anyhow::Result<String> {
    use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
    use ratatui::crossterm::terminal;
    use std::io::Write as _;
    struct Raw;
    impl Drop for Raw {
        fn drop(&mut self) {
            let _ = terminal::disable_raw_mode();
        }
    }
    let mut stderr = io::stderr();
    write!(stderr, "Management key: ")?;
    stderr.flush()?;
    terminal::enable_raw_mode().context("read the management key")?;
    let raw = Raw;
    let mut key = String::new();
    let result = loop {
        let Event::Key(press) = event::read()? else { continue };
        if press.kind == KeyEventKind::Release {
            continue;
        }
        match press.code {
            KeyCode::Enter => break Ok(key),
            KeyCode::Esc => break Err(anyhow::anyhow!("management key prompt cancelled")),
            KeyCode::Char('c' | 'd') if press.modifiers.contains(KeyModifiers::CONTROL) => {
                break Err(anyhow::anyhow!("management key prompt cancelled"));
            }
            KeyCode::Backspace => {
                key.pop();
            }
            KeyCode::Char(c) => key.push(c),
            _ => {}
        }
    };
    drop(raw);
    writeln!(stderr)?;
    result.map(|key| key.trim().to_owned())
}

/// Go's `appendCSV` flag function: each occurrence is split and appended.
fn csv_flags(values: &[String]) -> Vec<String> {
    values
        .iter()
        .flat_map(|v| discovery::cli::parse_interface_list(std::slice::from_ref(v)))
        .collect()
}

fn discover_options(
    timeout: i64,
    json: bool,
    service_type: String,
    config: &str,
    cli: (Vec<String>, Vec<String>),
) -> discovery::cli::Options {
    // CLI filters replace the file's, as in Go, so only an unfiltered scan needs them.
    let file = if cli.0.is_empty() && cli.1.is_empty() {
        discovery::cli::load_scan_filters(config).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(1)
        })
    } else {
        Default::default()
    };
    let (include, exclude) = discovery::cli::resolve_filters(cli, file);
    discovery::cli::Options {
        timeout: Duration::from_secs(timeout.clamp(0, 3600) as u64),
        json,
        service_type,
        include,
        exclude,
    }
}

/// A personal proxy spends its time waiting on upstreams, so two workers serve hundreds
/// of requests per second, and each extra worker can hold its own glibc arena. The
/// blocking pool keeps tokio's default ceiling (512): its threads start on demand and
/// exit after 10 idle seconds.
fn runtime(config: Option<&Config>) -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads(config))
        .enable_all()
        .build()
}

/// `TOKIO_WORKER_THREADS`, else `worker-threads` in `config` (read once at start), else
/// the smaller of the CPU count and 2.
fn worker_threads(config: Option<&Config>) -> usize {
    let positive = |n: u64| usize::try_from(n).ok().filter(|n| *n > 0);
    if let Ok(raw) = std::env::var("TOKIO_WORKER_THREADS") {
        match raw.trim().parse().ok().and_then(positive) {
            Some(n) => return n,
            None => tracing::warn!("ignoring TOKIO_WORKER_THREADS={raw:?}: not a positive number"),
        }
    }
    let configured = config
        .and_then(|c| c.document.get("worker-threads")?.as_u64())
        .and_then(positive);
    configured.unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get().min(2)))
}

/// Waits for Ctrl+C or SIGTERM, Go's `signal.NotifyContext(SIGINT, SIGTERM)`.
/// The log line is a stable contract: the container gate requires it to tell a
/// stop from a server that exited on its own.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler installs");
        let name = tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = term.recv() => "SIGTERM",
        };
        tracing::info!("shutdown signal received: {name}");
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// The config database: `--db`, else `CK_DB`, else `cooker.db` beside the config file.
fn db_path(flag: &str, config_file: &Path) -> PathBuf {
    if !flag.trim().is_empty() {
        return PathBuf::from(flag);
    }
    if let Some(env) = std::env::var_os("CK_DB").filter(|v| !v.is_empty()) {
        return PathBuf::from(env);
    }
    config_file
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .join("cooker.db")
}

/// What a first start imports when its YAML file is absent.
#[derive(Clone, Copy)]
enum FirstImport {
    /// An explicit `--config`: the file must exist.
    Required,
    /// The default `./config.yaml`: a fresh volume, or a blank file, starts with defaults and
    /// listens on `DEFAULT_PORT`. `Config::parse` itself still leaves an omitted port at 0.
    Defaults,
    /// `CK_DEPLOY=cloud`: an absent or blank file initializes an empty config, and the
    /// server waits for one.
    Cloud,
}

/// Opens the one config authority and imports YAML only before its first revision.
/// A required import that is missing fails before a new database file is created.
fn load_config(db: &Path, config_file: &Path, first: FirstImport) -> anyhow::Result<(Arc<ConfigStore>, Config)> {
    let cloud = matches!(first, FirstImport::Cloud);
    let default_config = || format!("server:\n  port: {}\n", ck_core::config::DEFAULT_PORT);
    let read_import = || match std::fs::read_to_string(config_file) {
        Ok(text) if cloud && text.trim().is_empty() => Ok(String::new()),
        Ok(text) if matches!(first, FirstImport::Defaults) && text.trim().is_empty() => Ok(default_config()),
        Ok(text) => Ok(text),
        Err(e) if e.kind() == io::ErrorKind::NotFound => match first {
            FirstImport::Required => {
                Err(e).with_context(|| format!("read first config import {}", config_file.display()))
            }
            FirstImport::Defaults => Ok(default_config()),
            FirstImport::Cloud => Ok(String::new()),
        },
        Err(e) => Err(e).with_context(|| format!("read first config import {}", config_file.display())),
    };
    let mut text = if db.exists() { None } else { Some(read_import()?) };
    let store = Arc::new(ConfigStore::open(db)?);
    if store.is_empty()? {
        let text = match text.take() {
            Some(text) => text,
            None => read_import()?,
        };
        let done = store
            .import_yaml(&text, "import", &format!("import {}", config_file.display()))
            .with_context(|| format!("import config {} into {}", config_file.display(), db.display()))?;
        tracing::info!(
            revision = done.rev,
            "initialized config database {}; later starts do not read YAML",
            db.display()
        );
    }
    let loaded = store
        .load()
        .map_err(|e| anyhow::anyhow!("load config database {}: {e}", db.display()))?;
    Ok((store, loaded.config))
}

/// Go's cloud-deploy check, of the config database.
fn cloud_config_present(store: &ConfigStore, config: &Config) -> bool {
    let (present, message) = if store.is_empty().unwrap_or(true) {
        (
            false,
            "Cloud deploy mode: No configuration detected; standing by for configuration",
        )
    } else if config.port == 0 {
        (
            false,
            "Cloud deploy mode: Configuration is empty or invalid; standing by for valid configuration",
        )
    } else {
        (true, "Cloud deploy mode: Configuration detected; starting service")
    };
    tracing::info!("{message}");
    present
}

/// Go `cmd.WaitForCloudDeploy`.
async fn wait_for_cloud_deploy() {
    tracing::info!(
        "Cloud deploy mode: No config found; standing by for configuration. API server is not started. Press Ctrl+C to exit."
    );
    shutdown_signal().await;
    tracing::info!("Cloud deploy mode: Shutdown signal received; exiting");
}

fn main() -> anyhow::Result<()> {
    ck_server::observability::logging::init();
    let raw: Vec<String> = std::env::args().collect();
    if config_cli::is_command(&raw[1..]) {
        return config_cli::execute(raw);
    }
    if raw.get(1).map(String::as_str) == Some("discover") {
        let args = DiscoverArgs::parse_from(raw.into_iter().skip(1));
        let cli = (csv_flags(&args.include), csv_flags(&args.exclude));
        let opts = discover_options(args.timeout, args.json, args.service_type, &args.config, cli);
        std::process::exit(runtime(None)?.block_on(discovery::cli::discover(&opts)));
    }
    // Parse errors, help and version exit here, before any database opens.
    let args = Args::parse_from(&raw);
    // First, so .env, the default config and relative paths resolve there.
    if let Some(dir) = &args.working_dir {
        std::env::set_current_dir(dir).with_context(|| format!("change to working directory {}", dir.display()))?;
    }
    // Next, relative to that directory, so every line from here on reaches the file:
    // sizing the runtime can warn.
    if let Some(path) = &args.log_file {
        ck_server::observability::logging::set_log_file(path.clone()).context("open process log")?;
    }
    // Discovery does not read .env.
    let discover_mode = args.discover || args.discover_json;
    // Before the runtime starts: setting variables is only sound single-threaded.
    if !discover_mode {
        dotenv::load_from_working_dir();
    }
    // An explicit online command must be selected before opening any local store.
    if !discover_mode && !args.management_base_url.trim().is_empty() && command_mode(&args) {
        if !online_command_supported(&args) {
            anyhow::bail!("this online provider login is not yet supported");
        }
        return runtime(None)?.block_on(online_command(&args));
    }
    let config_file = if args.config.is_empty() {
        PathBuf::from("config.yaml")
    } else {
        PathBuf::from(&args.config)
    };
    let db = db_path(&args.db, &config_file);
    let remote_client = args.tui && !args.standalone && !args.management_base_url.trim().is_empty();
    let loaded = if discover_mode || remote_client {
        None
    } else {
        let first = if std::env::var("CK_DEPLOY").is_ok_and(|v| v == "cloud") {
            FirstImport::Cloud
        } else if args.config.is_empty() {
            FirstImport::Defaults
        } else {
            FirstImport::Required
        };
        let loaded = load_config(&db, &config_file, first);
        // Like a failure inside `run`: the process log keeps why startup stopped.
        if let Err(error) = &loaded
            && args.log_file.is_some()
        {
            tracing::error!("{error:#}");
        }
        Some(loaded?)
    };
    let runtime = runtime(loaded.as_ref().map(|(_, config)| config))?;
    if discover_mode {
        let cli = (csv_flags(&args.discover_include), csv_flags(&args.discover_exclude));
        let opts = discover_options(
            args.discover_timeout,
            args.discover_json,
            args.discover_service_type.clone(),
            &args.config,
            cli,
        );
        std::process::exit(runtime.block_on(discovery::cli::discover(&opts)));
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    runtime.spawn(heap_trim::run());
    let file_log = args.log_file.is_some();
    let result = runtime.block_on(run(args, loaded));
    if file_log && let Err(error) = &result {
        tracing::error!("{error:#}");
    }
    result
}

/// glibc keeps the memory that request handling frees in its per-thread arenas and only
/// returns the top of each arena to the kernel, so after a burst of large prompts the
/// process stays near its peak size while most of that memory is free. `malloc_trim`
/// hands the free pages of every arena back. Trimming under load costs page faults on
/// the next requests, so it runs once the process goes quiet (under 50 ms of CPU in five
/// seconds), and at least once a minute while busy. It trims once after startup, then
/// parks until a response or a WebSocket turn ends: with no requests, no timer and no wakeup. The trim
/// itself runs on the blocking pool, off the two request threads. See
/// docs/BENCHMARKS.md (Claude soak).
///
/// Cost per request: the response body is boxed once more, and its drop does one
/// atomic load (plus a wakeup when the task is parked).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod heap_trim {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use axum::body::{Body, Bytes, HttpBody};

    const TICK: Duration = Duration::from_secs(5);
    const QUIET: Duration = Duration::from_millis(50);
    const MAX_GAP: u32 = 12;

    fn cpu_time() -> Duration {
        // SAFETY: getrusage writes only into the struct it is given.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
        tv(usage.ru_utime) + tv(usage.ru_stime)
    }

    pub async fn run() {
        loop {
            let (mut last, mut ticks) = (cpu_time(), 0);
            loop {
                tokio::time::sleep(TICK).await;
                let now = cpu_time();
                let quiet = now.saturating_sub(last) < QUIET;
                last = now;
                ticks += 1;
                if quiet || ticks >= MAX_GAP {
                    // Milliseconds to tens of ms on a large heap: not on a request thread.
                    // SAFETY: malloc_trim only releases the allocator's own free memory.
                    let _ = tokio::task::spawn_blocking(|| unsafe { libc::malloc_trim(0) }).await;
                    ticks = 0;
                }
                if quiet {
                    break;
                }
            }
            ck_wire::idle::park().await;
        }
    }

    /// Wakes the parked task when a response body is done (sent, or dropped with the
    /// connection).
    pub fn layer(app: axum::Router) -> axum::Router {
        app.layer(tower::util::MapResponseLayer::new(|res: axum::response::Response| {
            res.map(|body| Body::new(Done(body)))
        }))
    }

    struct Done(Body);

    impl HttpBody for Done {
        type Data = Bytes;
        type Error = axum::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, axum::Error>>> {
            Pin::new(&mut self.get_mut().0).poll_frame(cx)
        }

        fn is_end_stream(&self) -> bool {
            self.0.is_end_stream()
        }

        fn size_hint(&self) -> http_body::SizeHint {
            self.0.size_hint()
        }
    }

    impl Drop for Done {
        fn drop(&mut self) {
            ck_wire::idle::activity();
        }
    }
}

async fn run(args: Args, loaded: Option<(Arc<ConfigStore>, Config)>) -> anyhow::Result<()> {
    // An explicit remote URL makes `--tui` a pure management client: it needs no local
    // config or store (Go loads them first and can fail or wait there).
    // Login and import flags still run first, as in Go.
    if args.tui && !args.standalone && !args.management_base_url.trim().is_empty() && !command_mode(&args) {
        remote_tui(args.management_base_url.trim().to_owned(), args.password).await;
        return Ok(());
    }
    let (config_store, mut config) = loaded.context("local config was not initialized")?;
    let cloud = std::env::var("CK_DEPLOY").is_ok_and(|v| v == "cloud");
    let config_present = !cloud || cloud_config_present(&config_store, &config);
    let backend = initialize_credentials(config_store.clone(), &mut config)?;
    // Go ConfigureLogOutput and SetLogLevel, before any login command.
    ck_server::observability::logging::configure(&config);
    tracing::info!(commit = COMMIT, built = BUILD_DATE, "{VERSION}");
    if command_mode(&args) {
        return command(&args, &config, backend).await.map(|_| ());
    }
    if !config_present {
        wait_for_cloud_deploy().await;
        return Ok(());
    }
    if args.local_model && (!args.tui || args.standalone) {
        tracing::info!("Local model mode: using embedded model catalogs, remote model updates disabled");
    }
    if args.tui {
        if args.standalone {
            standalone_tui(config, config_store, backend, args.password, args.local_model).await;
        } else {
            let base = resolve_management_base_url(&args.management_base_url, &config);
            remote_tui(base, args.password).await;
        }
        return Ok(());
    }
    // Only the plain server detaches: discovery, login, import and TUI keep their
    // console.
    if args.log_file.is_some() {
        free_console();
    }
    serve(
        config,
        config_store,
        backend,
        args.password,
        args.local_model,
        shutdown_signal(),
        None,
    )
    .await
}

/// A direct Windows Run entry needs no persistent console or shell parent; logs go
/// to the `--log-file` instead.
fn free_console() {
    #[cfg(windows)]
    {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn FreeConsole() -> i32;
        }
        // SAFETY: detaches only this process from its console; no pointers.
        unsafe {
            FreeConsole();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuiltinCommand {
    Vertex,
    Siwc,
    Antigravity,
    Codex,
    Claude,
    Kimi,
    Xai,
    Devin,
    Meta,
}

/// Go's command priority also controls online eligibility, even with mixed flags.
fn selected_command(args: &Args) -> Option<BuiltinCommand> {
    [
        (!args.vertex_import.is_empty(), BuiltinCommand::Vertex),
        (!args.siwc_bind.is_empty(), BuiltinCommand::Siwc),
        (args.antigravity_login, BuiltinCommand::Antigravity),
        (args.codex_login || args.codex_device_login, BuiltinCommand::Codex),
        (args.claude_login, BuiltinCommand::Claude),
        (args.kimi_login || args.kimi_ai_login, BuiltinCommand::Kimi),
        (args.xai_login, BuiltinCommand::Xai),
        (args.devin_login, BuiltinCommand::Devin),
        (args.meta_login, BuiltinCommand::Meta),
    ]
    .into_iter()
    .find_map(|(enabled, command)| enabled.then_some(command))
}

fn online_command_supported(args: &Args) -> bool {
    matches!(
        selected_command(args),
        Some(
            BuiltinCommand::Vertex
                | BuiltinCommand::Codex
                | BuiltinCommand::Siwc
                | BuiltinCommand::Kimi
                | BuiltinCommand::Xai
                | BuiltinCommand::Meta
                | BuiltinCommand::Devin
                | BuiltinCommand::Antigravity
                | BuiltinCommand::Claude
        )
    )
}

/// Go's `commandMode`: a login or import flag is set.
fn command_mode(args: &Args) -> bool {
    selected_command(args).is_some()
}

/// Go's TUI client mode: a pure management client; the server runs elsewhere.
async fn remote_tui(base: String, password: String) {
    let run = tokio::task::spawn_blocking(move || tui::run(&base, &password, None, io::stdout())).await;
    if let Err(e) = run.map_err(io::Error::other).and_then(|r| r) {
        eprintln!("TUI error: {e}");
    }
}

/// Go `resolveManagementBaseURL`: the flag, then `remote-management.base-url`, then
/// the local server's port (8317 when unset).
fn resolve_management_base_url(flag: &str, config: &Config) -> String {
    let configured = config
        .document
        .get("management")
        .and_then(|m| m.get("base-url"))
        .and_then(serde_yaml_ng::Value::as_str)
        .unwrap_or_default();
    management_base_url(flag, configured, i64::from(config.port))
}

fn management_base_url(flag: &str, configured: &str, port: i64) -> String {
    for url in [flag, configured] {
        if !url.trim().is_empty() {
            return url.trim().to_owned();
        }
    }
    format!("http://127.0.0.1:{}", if port > 0 { port } else { 8317 })
}

/// Go's standalone TUI points stdout and stderr at /dev/null while it runs and draws on
/// the original stdout; dropping this restores both.
#[cfg(unix)]
struct QuietStdio {
    out: libc::c_int,
    err: libc::c_int,
}

#[cfg(unix)]
impl QuietStdio {
    /// Silences fds 1 and 2; returns the guard and a handle on the original stdout.
    fn start() -> io::Result<(Self, std::fs::File)> {
        use std::os::fd::{AsRawFd, FromRawFd};
        io::Write::flush(&mut io::stdout())?;
        let null = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
        // SAFETY: plain fd duplication; each new fd is owned by the guard or the File.
        unsafe {
            let (out, err, tty) = (libc::dup(1), libc::dup(2), libc::dup(1));
            if out < 0 || err < 0 || tty < 0 {
                return Err(io::Error::last_os_error());
            }
            libc::dup2(null.as_raw_fd(), 1);
            libc::dup2(null.as_raw_fd(), 2);
            Ok((QuietStdio { out, err }, std::fs::File::from_raw_fd(tty)))
        }
    }
}

#[cfg(unix)]
impl Drop for QuietStdio {
    fn drop(&mut self) {
        // SAFETY: restores the fds saved in `start` and closes the saved copies.
        unsafe {
            libc::dup2(self.out, 1);
            libc::dup2(self.err, 2);
            libc::close(self.out);
            libc::close(self.err);
        }
    }
}

/// ponytail: Windows keeps the console handles. Go swaps only its `os.Stdout`
/// variable there too; the log lines already go to the hook, so only a stray direct
/// print could reach the screen.
#[cfg(not(unix))]
struct QuietStdio;

#[cfg(not(unix))]
impl QuietStdio {
    fn start() -> io::Result<(Self, io::Stdout)> {
        Ok((QuietStdio, io::stdout()))
    }
}

/// Go's `--tui --standalone`: the server runs in this process with a local management
/// password (`tui-<pid>-<nanos>` unless -password is set), its log lines feed the logs
/// tab, and it stops after the TUI quits.
async fn standalone_tui(
    config: Config,
    config_store: Arc<ConfigStore>,
    backend: Arc<CredentialDb>,
    password: String,
    local_model: bool,
) {
    let hook = ck_server::observability::logging::capture(2000);
    let (quiet, out): (Option<QuietStdio>, Box<dyn io::Write + Send>) = match QuietStdio::start() {
        Ok((guard, tty)) => (Some(guard), Box::new(tty)),
        Err(_) => (None, Box::new(io::stdout())),
    };
    let password = if password.is_empty() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        format!("tui-{}-{nanos}", std::process::id())
    } else {
        password
    };
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let shutdown = async {
        let _ = stopped.await;
    };
    let (listening, listener) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(
        config,
        config_store,
        backend,
        password.clone(),
        local_model,
        shutdown,
        Some(listening),
    ));
    // No listener means serve failed; its error is logged below.
    let base = match listener.await {
        Ok(listener) => standalone_base_url(listener),
        Err(_) => Err("embedded server is not ready".to_owned()),
    };
    let result = match &base {
        Ok(base) if tui::wait_ready(base, &password).await => {
            let base = base.clone();
            let run = tokio::task::spawn_blocking(move || tui::run(&base, &password, Some(hook), out)).await;
            run.map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| e.to_string()))
        }
        Ok(_) => Err("embedded server is not ready".to_owned()),
        Err(e) => Err(e.clone()),
    };
    // Restores stdout and stderr on Unix; outside Unix nothing was redirected.
    #[cfg_attr(not(unix), allow(clippy::drop_non_drop))]
    drop(quiet);
    ck_server::observability::logging::release();
    if let Err(e) = result {
        eprintln!("TUI error: {e}");
    }
    let _ = stop.send(());
    if let Ok(Err(e)) = server.await {
        tracing::error!("{e:#}");
    }
}

fn initialize_credentials(store: Arc<ConfigStore>, config: &mut Config) -> anyhow::Result<Arc<CredentialDb>> {
    let backend = CredentialDb::initialize(store)?;
    config.auth_dir = backend.auth_dir();
    Ok(backend)
}

fn login_save(backend: Arc<CredentialDb>) -> (ck_exec::login_store::LoginSave, Arc<std::sync::Mutex<Option<String>>>) {
    let failure = Arc::new(std::sync::Mutex::new(None));
    let failed = failure.clone();
    let save: ck_exec::login_store::LoginSave = Arc::new(move |paths, write| {
        let committed = (|| -> anyhow::Result<()> {
            let mut paths = paths
                .iter()
                .map(|path| ck_server::store::credential_store::credential_path_key(path))
                .collect::<io::Result<Vec<_>>>()?;
            paths.sort();
            paths.dedup();
            let _locks = paths
                .iter()
                .map(|path| ck_exec::codex_siwc_bind::lock_file(path).map_err(anyhow::Error::msg))
                .collect::<anyhow::Result<Vec<_>>>()?;
            backend.write_files(&paths, || write().map_err(anyhow::Error::msg))
        })();
        committed.map_err(|error| {
            let message = format!("commit CLI credentials to SQLite: {error:#}");
            *failed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(message.clone());
            message
        })
    });
    (save, failure)
}

/// Whether a command flag handled this invocation.
enum Ran {
    Nothing,
    Handled,
    Login(PathBuf, &'static str),
    Imported(PathBuf),
    Bound(PathBuf),
}

async fn command(args: &Args, config: &Config, backend: Arc<CredentialDb>) -> anyhow::Result<Ran> {
    let (save, failure) = login_save(backend);
    let result = command_inner(args, config, save).await;
    if let Some(error) = failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
        anyhow::bail!(error);
    }
    let result = result?;
    report_command(&result);
    Ok(result)
}

fn report_command(result: &Ran) {
    match result {
        Ran::Login(path, provider) => {
            println!("Authentication saved to {}", path.display());
            println!("{provider} authentication successful!");
        }
        Ran::Imported(path) => println!("Vertex credentials imported: {}", path.display()),
        Ran::Bound(path) => {
            println!("SIWC credentials bound and saved to {}", path.display());
            println!("Sign in with ChatGPT authentication successful!");
        }
        _ => {}
    }
}

fn hydrate_login_source(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    if path.parent() != Some(root) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "staged target is outside private scratch",
        ));
    }
    let mut file = tempfile::NamedTempFile::new_in(root)?;
    file.write_all(bytes)?;
    file.persist(path).map(|_| ()).map_err(|error| error.error)
}

fn hydrate_claude_sources(
    root: &Path,
    sources: &std::collections::BTreeMap<String, tui::CredentialSource>,
) -> io::Result<()> {
    use ck_core::gostr::GoStr as _;
    // Every name from credential_file_name has this prefix; the login merges the
    // existing file of its own name.
    for (name, source) in sources
        .iter()
        .filter(|(name, _)| name.go_lower().starts_with("claude-"))
    {
        hydrate_login_source(root, &root.join(name), &source.bytes)?;
    }
    Ok(())
}

/// Resolve a caller's logical destination without consulting client filesystem state.
fn online_siwc_target(target: &str, sources: &tui::CredentialSources) -> anyhow::Result<String> {
    let raw = target.trim();
    let supplied = Path::new(raw);
    let name = if supplied.is_absolute() {
        anyhow::ensure!(
            sources.auth_dir.is_absolute(),
            "use a managed filename on this client platform"
        );
        let path = ck_server::store::credential_store::credential_path_key(supplied)?;
        let root = ck_server::store::credential_store::credential_path_key(&sources.auth_dir)?;
        anyhow::ensure!(
            path.parent() == Some(root.as_path()),
            "SIWC target must belong to the running owner's managed directory"
        );
        path.file_name()
            .and_then(|name| name.to_str())
            .context("invalid SIWC target name")?
            .to_owned()
    } else {
        anyhow::ensure!(
            !raw.is_empty() && !raw.contains(['/', '\\', ':']),
            "SIWC needs a managed filename or an absolute owner target"
        );
        raw.to_owned()
    };
    anyhow::ensure!(
        sources.documents.contains_key(&name),
        "SIWC target is not an existing managed credential"
    );
    Ok(name)
}

/// Inspected built-in providers call this on their owned blocking writer.
fn online_login_save(
    scratch: Arc<tempfile::TempDir>,
    client: Arc<tui::Client>,
    sources: std::collections::BTreeMap<String, tui::CredentialSource>,
    runtime: tokio::runtime::Handle,
) -> (ck_exec::login_store::LoginSave, Arc<std::sync::Mutex<Option<String>>>) {
    let failure = Arc::new(std::sync::Mutex::new(None));
    let failed = failure.clone();
    let source_cursor = std::sync::Mutex::new(sources);
    let operation = std::sync::Mutex::new(());
    let save: ck_exec::login_store::LoginSave = Arc::new(move |paths, write| {
        let result: Result<(), String> = (|| {
            // Own the scratch render/read/ack transaction, separately from its cursor.
            // Reject overlap/reentry rather than blocking a callback on itself.
            let _operation = match operation.try_lock() {
                Ok(permit) => permit,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    return Err("online credential writer is already running".into());
                }
            };
            let paths: Vec<_> = paths
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            let mut names = Vec::with_capacity(paths.len());
            for path in &paths {
                if path.parent() != Some(scratch.path()) {
                    return Err("online credential writer escaped its private staging directory".into());
                }
                names.push(
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .ok_or("invalid staged credential name")?
                        .to_owned(),
                );
            }
            let sources: std::collections::BTreeMap<_, _> = {
                let cursor = source_cursor.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                names
                    .iter()
                    .filter_map(|name| cursor.get(name).map(|source| (name.clone(), source.clone())))
                    .collect()
            };
            // Hydrate only declared inputs before the provider's final merge/write.
            for (path, name) in paths.iter().zip(&names) {
                if let Some(source) = sources.get(name) {
                    hydrate_login_source(scratch.path(), path, &source.bytes).map_err(|e| e.to_string())?;
                }
            }
            write()?;
            let mut documents = Vec::new();
            let mut deletions = Vec::new();
            for (path, name) in paths.iter().zip(names) {
                match std::fs::read(path) {
                    Ok(bytes) => documents.push((name, bytes)),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        // A declared, captured destination removed by a successful writer.
                        if sources.contains_key(&name) {
                            deletions.push(name);
                        }
                    }
                    Err(_) => return Err("read rendered credential failed".to_owned()),
                }
            }
            // FileTokenStore can deliberately skip a disabled, absent auth. There
            // is no durable effect to acknowledge when every declared path is such a no-op.
            if documents.is_empty() && deletions.is_empty() {
                return Ok(());
            }
            // The callback returns Ok only after one durable mixed-batch acknowledgement.
            runtime.block_on(async {
                if deletions.is_empty() {
                    client.save_credentials(&documents, Some(&sources)).await
                } else {
                    client
                        .apply_credential_changes(&documents, &deletions, Some(&sources))
                        .await
                }
            })?;
            // Advance only to our acknowledged bytes, never to a fresh remote read.
            // No cursor/state lock is held during rendering, HTTP or callback destruction.
            use sha2::Digest as _;
            let updates = documents
                .into_iter()
                .map(|(name, bytes)| {
                    let sha256 = format!("{:x}", sha2::Sha256::digest(&bytes));
                    (name, tui::CredentialSource { bytes, sha256 })
                })
                .collect::<Vec<_>>();
            let mut cursor = source_cursor.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            cursor.extend(updates);
            for name in deletions {
                cursor.remove(&name);
            }
            Ok(())
        })();
        if let Err(error) = &result {
            *failed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
        }
        result
    });
    (save, failure)
}

async fn online_command(args: &Args) -> anyhow::Result<()> {
    let password = online_management_key(args.password.clone()).await?;
    let client = Arc::new(tui::Client::new(&args.management_base_url, &password));
    client.credential_admission().await.map_err(anyhow::Error::msg)?;
    let mut config = if matches!(
        selected_command(args),
        Some(BuiltinCommand::Vertex | BuiltinCommand::Codex | BuiltinCommand::Claude)
    ) {
        Config::parse("")?
    } else {
        client.login_config().await.map_err(anyhow::Error::msg)?
    };
    let sources = client.credential_sources().await.map_err(anyhow::Error::msg)?;
    let scratch = Arc::new(tempfile::Builder::new().prefix("cooker-login-").tempdir()?);
    let mut args = args.clone();
    let siwc_report = if selected_command(&args) == Some(BuiltinCommand::Siwc) {
        let name = online_siwc_target(&args.siwc_bind, &sources)?;
        let report = PathBuf::from(args.siwc_bind.trim());
        let bytes = sources.documents.get(&name).expect("validated source").bytes.clone();
        let owner = scratch.clone();
        let path = scratch.path().join(&name);
        tokio::task::spawn_blocking(move || hydrate_login_source(owner.path(), &path, &bytes))
            .await
            .context("prepare private SIWC input")??;
        args.siwc_bind = name;
        Some(report)
    } else {
        None
    };
    let sources = if selected_command(&args) == Some(BuiltinCommand::Claude) {
        let owner = scratch.clone();
        tokio::task::spawn_blocking(move || {
            hydrate_claude_sources(owner.path(), &sources.documents)?;
            Ok::<_, io::Error>(sources)
        })
        .await
        .context("prepare private Claude alias inputs")??
    } else {
        sources
    };
    let (save, failure) = online_login_save(
        scratch.clone(),
        client,
        sources.documents,
        tokio::runtime::Handle::current(),
    );
    config.auth_dir = scratch.path().to_owned();
    let result = if selected_command(&args) == Some(BuiltinCommand::Vertex) {
        let key = args.vertex_import.clone();
        let prefix = args.vertex_import_prefix.clone();
        let owner = scratch.clone();
        tokio::task::spawn_blocking(move || Ok(import_vertex(owner.path(), &key, &prefix, &save)))
            .await
            .context("render online Vertex credentials")?
    } else {
        command_inner(&args, &config, save).await
    };
    if let Some(error) = failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
        anyhow::bail!(error);
    }
    let result = match result? {
        Ran::Imported(path) => Ran::Imported(path.file_name().context("missing rendered name")?.into()),
        Ran::Login(path, provider) => Ran::Login(path.file_name().context("missing rendered name")?.into(), provider),
        Ran::Bound(_) => Ran::Bound(siwc_report.context("missing SIWC destination")?),
        other => other,
    };
    Arc::try_unwrap(scratch)
        .map_err(|_| anyhow::anyhow!("credential writer has not settled"))?
        .close()
        .context("remove private client credential staging")?;
    report_command(&result);
    Ok(())
}

fn import_vertex(auth_dir: &Path, key: &str, prefix: &str, save: &ck_exec::login_store::LoginSave) -> Ran {
    match ck_exec::vertex_auth::import(auth_dir, key, prefix, Some(save)) {
        Ok(path) => Ran::Imported(path),
        Err(error) => {
            tracing::error!("{error}");
            Ran::Handled
        }
    }
}

/// Go's command modes, in Go's order.
async fn command_inner(args: &Args, config: &Config, save: ck_exec::login_store::LoginSave) -> anyhow::Result<Ran> {
    let command = selected_command(args);
    // Go DoVertexImport: failures are logged and the command still exits normally.
    if command == Some(BuiltinCommand::Vertex) {
        return Ok(import_vertex(
            &config.auth_dir,
            &args.vertex_import,
            &args.vertex_import_prefix,
            &save,
        ));
    }
    if command == Some(BuiltinCommand::Siwc) {
        // Go DoSIWCBind: failures are reported and the command still exits normally,
        // except a busy callback port (ErrPortInUse's code 13).
        let options = ck_exec::codex_oauth::LoginOptions {
            no_browser: args.no_browser,
            callback_port: args.oauth_callback_port,
        };
        match ck_exec::codex_siwc_bind::bind(config, &args.siwc_bind, &options, Some(save)).await {
            Ok(path) => return Ok(Ran::Bound(path)),
            Err(ck_exec::codex_siwc_bind::BindError::Log(line)) => tracing::error!("{line}"),
            Err(ck_exec::codex_siwc_bind::BindError::Print(line)) => println!("{line}"),
            Err(ck_exec::codex_siwc_bind::BindError::PortInUse) => {
                tracing::error!(
                    "The required port is already in use. Please close any applications using port 3000 and try again."
                );
                std::process::exit(13);
            }
        }
        return Ok(Ran::Handled);
    }
    if command == Some(BuiltinCommand::Antigravity) {
        // Go DoAntigravityLogin: a failure is logged and the command still exits normally.
        let options = ck_exec::antigravity_auth::LoginOptions {
            no_browser: args.no_browser,
            callback_port: args.oauth_callback_port,
        };
        match ck_exec::antigravity_auth::login(&config.auth_dir, config, &options, Some(save)).await {
            Ok((path, _)) => return Ok(Ran::Login(path, "Antigravity")),
            Err(error) => tracing::error!("Antigravity authentication failed: {error}"),
        }
        return Ok(Ran::Handled);
    }
    if command == Some(BuiltinCommand::Codex) {
        let options = ck_exec::codex_oauth::LoginOptions {
            no_browser: args.no_browser,
            callback_port: match args.oauth_callback_port {
                0 => ck_exec::codex_oauth::DEFAULT_CALLBACK_PORT,
                port => port,
            },
        };
        let path = if args.codex_login {
            ck_exec::codex_oauth::login(&config.auth_dir, &options, Some(save)).await?
        } else {
            ck_exec::codex_oauth::device_login(&config.auth_dir, &options, Some(save)).await?
        };
        return Ok(Ran::Login(path, "Codex"));
    }
    if command == Some(BuiltinCommand::Claude) {
        let options = ck_exec::claude_login::LoginOptions {
            no_browser: args.no_browser,
            callback_port: match args.oauth_callback_port {
                0 => ck_exec::claude_login::DEFAULT_CALLBACK_PORT,
                port => port,
            },
        };
        let error = match ck_exec::claude_login::login(&config.auth_dir, &options, Some(save)).await {
            Ok(path) => return Ok(Ran::Login(path, "Claude")),
            Err(error) => error,
        };
        let message = String::from_utf8_lossy(&error.body).into_owned();
        // DoClaudeLogin: a busy callback port exits with ErrPortInUse's code 13.
        if message.starts_with(ck_exec::claude_login::PORT_IN_USE) {
            tracing::error!(
                "The required port is already in use. Please close any applications using port 3000 and try again."
            );
            std::process::exit(13);
        }
        println!("Claude authentication failed: {message}");
        return Ok(Ran::Handled);
    }
    if command == Some(BuiltinCommand::Kimi) {
        let provider = if args.kimi_login { "kimi" } else { "kimi-ai" };
        let path = ck_exec::kimi_auth::login(provider, config, args.no_browser, Some(save)).await?;
        return Ok(Ran::Login(path, if args.kimi_login { "Kimi" } else { "Kimi.ai" }));
    }
    if command == Some(BuiltinCommand::Xai) {
        // Go DoXAILogin: a failure is logged and the command still exits normally.
        match ck_exec::xai_auth::login(config, args.no_browser, Some(save)).await {
            Ok(path) => return Ok(Ran::Login(path, "xAI")),
            Err(error) => tracing::error!("xAI authentication failed: {}", String::from_utf8_lossy(&error.body)),
        }
        return Ok(Ran::Handled);
    }
    if command == Some(BuiltinCommand::Devin) {
        // Go DoDevinLogin: a failure is logged and the command still exits normally.
        return Ok(
            match ck_exec::devin_auth::login(config, args.no_browser, args.oauth_callback_port, Some(save)).await {
                Some(path) => Ran::Login(path, "Devin"),
                None => Ran::Handled,
            },
        );
    }
    if command == Some(BuiltinCommand::Meta) {
        let path = ck_exec::meta_auth::login(config, args.no_browser, Some(save)).await?;
        return Ok(Ran::Login(path, "Meta"));
    }
    Ok(Ran::Nothing)
}

/// The standalone TUI's server address: the bound one, or loopback for a wildcard bind.
/// The local password works only from loopback, so other hosts are refused, and so is
/// TLS, which would need the server certificate to be trusted for that address.
fn standalone_base_url((addr, tls): (SocketAddr, bool)) -> Result<String, String> {
    if tls {
        return Err("the standalone TUI does not support server.tls; turn it off or run the TUI as a client".into());
    }
    let ip = match addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.into(),
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => Ipv6Addr::LOCALHOST.into(),
        ip if ip.is_loopback() => ip,
        ip => {
            return Err(format!(
                "the standalone TUI needs a loopback or wildcard host, not {ip}; run the TUI as a client"
            ));
        }
    };
    Ok(format!("http://{}", SocketAddr::new(ip, addr.port())))
}

/// The served routes.
fn app(rt: &Arc<Runtime>, management: &Arc<ck_server::management::api::Management>) -> axum::Router {
    let app = ck_server::app(rt.clone(), ck_server::management::api::router(management.clone()));
    let app = ck_server::net::safe_mode::router(rt, app);
    // Go applies its CORS middleware to every route, not only management.
    let app = app.layer(axum::middleware::from_fn(ck_server::management::api::cors));
    let app = ck_server::observability::request_logging::router(management, app);
    let app = ck_server::http::body_limit::layer(rt, app);
    ck_server::observability::api::router(rt, app)
}

/// `listening` (standalone TUI only) receives the bound address and whether TLS is on.
async fn serve(
    config: Config,
    config_store: Arc<ConfigStore>,
    backend: Arc<CredentialDb>,
    password: String,
    local_model: bool,
    shutdown: impl std::future::Future<Output = ()>,
    listening: Option<tokio::sync::oneshot::Sender<(SocketAddr, bool)>>,
) -> anyhow::Result<()> {
    if config.api_keys.is_empty() {
        tracing::warn!("access.api-keys is empty: the proxy API is open to anyone who can reach it");
    }
    // Stored credential documents and config API keys, synthesized as Go's watcher does.
    let scan = backend.scan(&config)?;
    let mut credentials = scan.credentials;
    credentials.extend(ck_core::config::credentials::from_config(&config));
    let read = scan.read;
    tracing::info!(credentials = credentials.len(), "credentials loaded");
    let listener =
        bind(&config.host, config.port).with_context(|| format!("binding {}:{}", config.host, config.port))?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    // Go `Server.Start`: TLS is validated after the listener is open.
    let tls = ck_server::net::listener::tls_acceptor(&config)?;
    tracing::info!(addr = %listener.local_addr()?, tls = tls.is_some(), "listening");
    let standalone = listening.is_some();
    if let Some(listening) = listening {
        let _ = listening.send((listener.local_addr()?, tls.is_some()));
    }
    let executors = Executors {
        claude: ClaudeExecutor::new(DEFAULT_BASE_URL)?,
        codex: ck_exec::codex::CodexExecutor::new()?,
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    #[cfg(test)]
    let rt = ck_server::testing::runtime_with_db(config, credentials, executors, backend);
    #[cfg(not(test))]
    let rt = Runtime::new(config, credentials, executors, backend);
    let rt = Arc::new(rt);
    // What each file credential was loaded from, for Go-exact management saves.
    rt.store().note_loaded(&read);
    rt.set_local_model(local_model);
    // Go `misc.StartAntigravityVersionUpdater` and `startModelCatalogUpdaters`.
    ck_server::inference::model_updater::start(&rt);
    // Model router benchmarks (cooker only); idle without `artificial-analysis.api-key`.
    ck_server::inference::aa::start(&rt);
    // Translators and thinking validation read model capabilities through the global
    // overlay; without it they see only the static catalog.
    ck_server::install_registry(&rt);
    rt.start_auto_refresh();
    let options = ck_server::management::api::Options {
        local_password: password,
        standalone,
        ..Default::default()
    };
    let management = ck_server::management::api::Management::with_options(rt.clone(), config_store, options);
    let pprof = ck_server::observability::pprof::Server::start(&rt);
    let credential_notifications = ck_server::management::watching::start(&management);
    // The served transport decides `tls=`: HTTPS exactly when server.tls loaded.
    let advertiser = discovery::advertise::Advertiser::spawn(rt.subscribe_config(), tls.is_some());
    let app = app(&rt, &management);
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    let app = heap_trim::layer(app);
    // Go's listener also serves the Redis protocol (usage queue) to management clients.
    let mut server = Box::pin(ck_server::net::listener::serve_with_resp(
        listener,
        app,
        tls,
        Some(management.clone()),
    ));
    let mut server_done = false;
    let served = tokio::select! {
        r = &mut server => {
            server_done = true;
            r.map_err(anyhow::Error::from)
        },
        _ = shutdown => Ok(()),
    };
    management.begin_shutdown();
    rt.begin_shutdown();
    credential_notifications.stop();
    // Keep Go's pprof/discovery drain order, but stop configuration and grant
    // admission before producers can race the final persistence drain.
    let cleanup = async {
        pprof.shutdown().await;
        advertiser.shutdown().await;
    };
    tokio::pin!(cleanup);
    if server_done {
        cleanup.await;
    } else {
        tokio::select! {
            _ = &mut cleanup => {},
            _ = &mut server => cleanup.await,
        }
    }
    // Server.Stop closes the HTTP server without draining (http.Server.Close).
    drop(server);
    management.shutdown().await;
    let notifications = credential_notifications
        .shutdown()
        .await
        .context("settle credential notification work");
    let persistence = rt.finish_shutdown().await;
    // Settlement can retry a pending configuration, which reconfigures logging; the
    // process-wide log directory cleaner is stopped and joined only after it.
    if let Err(error) = tokio::task::spawn_blocking(ck_server::observability::logging::stop_cleaner).await {
        tracing::error!("log directory cleaner did not stop: {error}");
    }
    notifications?;
    persistence?;
    served
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_management_key_prefers_flag_then_environment_then_terminal_prompt() {
        let never = || -> anyhow::Result<String> { panic!("must not prompt") };
        let env = |v: &str| Some(std::ffi::OsString::from(v));
        assert_eq!(resolve_management_key("flag", env("env"), true, never).unwrap(), "flag");
        assert_eq!(resolve_management_key("", env("env"), true, never).unwrap(), "env");
        assert_eq!(resolve_management_key("", env(""), false, never).unwrap(), "");
        assert_eq!(resolve_management_key("", None, false, never).unwrap(), "");
        assert_eq!(
            resolve_management_key("", env(""), true, || Ok("typed".into())).unwrap(),
            "typed"
        );
        let cancelled = resolve_management_key("", None, true, || anyhow::bail!("management key prompt cancelled"));
        assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt as _;
            let invalid = Some(std::ffi::OsString::from_vec(vec![0xff]));
            assert!(resolve_management_key("", invalid, true, never).is_err());
        }
    }

    fn credential_test_dir() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "cooker-cli-credential-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn online_builtin_flows_wait_for_durable_save_and_preserve_source_fences() {
        use axum::middleware::Next;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let root = credential_test_dir();
        let store = Arc::new(ck_server::testing::config_store(&root.join("service.db")).unwrap());
        store.import_yaml("oauth: {}\n", "test", "device flow").unwrap();
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let mut config = store.load().unwrap().config;
        config.auth_dir = db.auth_dir();
        let rt = Arc::new(ck_server::testing::runtime_with_db(
            config,
            Vec::new(),
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:9").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            db.clone(),
        ));
        let state = ck_server::management::api::Management::with_options(
            rt.clone(),
            store.clone(),
            ck_server::management::api::Options {
                management_password: Some("device-fixture-key".into()),
                ..Default::default()
            },
        );
        let go: serde_json::Value =
            serde_json::from_str(include_str!("../../ck-exec/tests/fixtures/codex_go.json")).unwrap();
        let name = go["oauth"]["exchanges"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "device")
            .unwrap()["result"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let token = serde_json::json!({"access_token":"at-FAKE", "refresh_token":"rt-FAKE",
            "id_token":go["oauth"]["id_token"], "token_type":"Bearer", "expires_in":3600,
            "scope":"openid chatgpt.tokens.use.direct"});
        let grants = Arc::new(AtomicUsize::new(0));
        let observed = grants.clone();
        let kimi_grants = Arc::new(AtomicUsize::new(0));
        let kimi_observed = kimi_grants.clone();
        let claude_grants = Arc::new(AtomicUsize::new(0));
        let claude_observed = claude_grants.clone();
        let reply_ready = Arc::new(tokio::sync::Notify::new());
        let reply_release = Arc::new(tokio::sync::Notify::new());
        let first = Arc::new(AtomicBool::new(true));
        let ready = reply_ready.clone();
        let release = reply_release.clone();
        let app = ck_server::management::api::router(state.clone())
            .route("/mock/usercode", axum::routing::post(|| async { axum::Json(serde_json::json!({
                "device_auth_id":"loopback", "usercode":"FAKE-CODE", "interval":"1"})) }))
            .route("/mock/device", axum::routing::post(|| async { axum::Json(serde_json::json!({
                "authorization_code":"loopback-code", "code_verifier":"loopback-verifier", "code_challenge":"loopback-challenge"})) }))
            .route("/mock/token", axum::routing::post(move || {
                let token = token.clone(); let observed = observed.clone();
                async move { observed.fetch_add(1, Ordering::SeqCst); axum::Json(token) }
            }))
            .route("/mock/claude/token", axum::routing::post(move || {
                let observed = claude_observed.clone();
                async move { observed.fetch_add(1, Ordering::SeqCst); axum::Json(serde_json::json!({
                    "access_token":"sk-ant-oat-login-fake", "refresh_token":"fake-r", "expires_in":3600,
                    "account":{"uuid":"acct-1","email_address":"a@example.invalid"}, "organization":{"uuid":"org-1","name":"Org"}})) }
            }))
            .route("/mock/claude/profile", axum::routing::get(|| async { axum::Json(serde_json::json!({
                "account":{"uuid":"acct-1","email":"a@example.invalid"}, "organization":{"uuid":"org-1","name":"Org"}})) }))
            .route("/mock/claude/roles", axum::routing::get(|| async { axum::Json(serde_json::json!({})) }))
            .route("/api/oauth/device_authorization", axum::routing::post(|| async { axum::Json(serde_json::json!({
                "device_code":"loopback", "user_code":"FAKE-KIMI", "verification_uri":"http://127.0.0.1:9/never-opened",
                "expires_in":60, "interval":5})) }))
            .route("/api/oauth/token", axum::routing::post(move || {
                let observed = kimi_observed.clone();
                async move { observed.fetch_add(1, Ordering::SeqCst); axum::Json(serde_json::json!({
                    "access_token":"kimi-FAKE", "refresh_token":"kimi-FAKE-REFRESH", "token_type":"Bearer", "expires_in":3600})) }
            }))
            .layer(axum::middleware::from_fn(move |request: axum::extract::Request, next: Next| {
                let (ready, release, first) = (ready.clone(), release.clone(), first.clone());
                async move {
                    let batch = request.uri().path() == "/admin/v1/credentials/batch";
                    let response = next.run(request).await;
                    if batch && first.swap(false, Ordering::SeqCst) {
                        ready.notify_one(); release.notified().await;
                    }
                    response
                }
            }));
        let xai_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../ck-exec/tests/fixtures/xai_auth_go.json")).unwrap();
        let xai_case = xai_fixture["login"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "subject_only_merges_existing_file")
            .unwrap();
        let xai_grants = Arc::new(AtomicUsize::new(0));
        let mut app = app;
        for reply in xai_case["script"].as_array().unwrap() {
            let body = reply["body"].as_str().unwrap().to_owned();
            let token = reply["path"] == "/oauth2/token";
            let observed = xai_grants.clone();
            app = app.route(
                reply["path"].as_str().unwrap(),
                axum::routing::any(move || {
                    let (body, observed) = (body.clone(), observed.clone());
                    async move {
                        if token {
                            observed.fetch_add(1, Ordering::SeqCst);
                        }
                        ([(axum::http::header::CONTENT_TYPE, "application/json")], body)
                    }
                }),
            );
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap()
        });
        let client = Arc::new(tui::Client::new(&base, "device-fixture-key"));
        let target = db.auth_dir().join(&name);
        let seed = br#"{"type":"codex","disabled":false,"access_token":"old-fixture","opaque":{"keep":[1,true]}}"#;
        for case in ["success", "storage", "replaced", "deleted"] {
            db.save(&target, seed).unwrap();
            client.credential_admission().await.unwrap();
            let sources = client.credential_sources().await.unwrap();
            let replacement = br#"{"type":"codex","replacement":"never overwrite"}"#;
            if case == "replaced" {
                db.save(&target, replacement).unwrap();
            }
            if case == "deleted" {
                db.delete(&target).unwrap();
            }
            let blocked = (case == "storage").then(|| ck_server::testing::block_checkpoint_install(&store));
            let scratch = Arc::new(
                tempfile::Builder::new()
                    .prefix("cooker-login-")
                    .tempdir_in(&root)
                    .unwrap(),
            );
            let (save, failure) = online_login_save(
                scratch.clone(),
                client.clone(),
                sources.documents,
                tokio::runtime::Handle::current(),
            );
            let oauth = ck_exec::codex_oauth::CodexOAuth::with_endpoints(
                wreq::Client::builder().no_proxy().build().unwrap(),
                &format!("{base}/mock/token"),
                &format!("{base}/mock/usercode"),
                &format!("{base}/mock/device"),
            );
            let dir = scratch.path().to_owned();
            let provider = tokio::spawn(async move {
                ck_exec::codex_oauth::device_login_with(
                    &dir,
                    &ck_exec::codex_oauth::LoginOptions {
                        callback_port: 0,
                        no_browser: true,
                    },
                    Some(save),
                    oauth,
                )
                .await
            });
            if case == "success" {
                tokio::time::timeout(std::time::Duration::from_secs(8), reply_ready.notified())
                    .await
                    .unwrap();
                assert!(
                    !provider.is_finished(),
                    "a provider save must not return Ok before its API acknowledgement"
                );
                reply_release.notify_one();
            }
            let result = provider.await.unwrap();
            let error = failure.lock().unwrap().take();
            match case {
                "success" => {
                    result.unwrap();
                    assert!(error.is_none());
                    let bytes = db.read(&target).unwrap().unwrap();
                    let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(doc["access_token"], "at-FAKE");
                    assert_eq!(doc["opaque"]["keep"], serde_json::json!([1, true]));
                    let copy = root.join("portable.db");
                    std::fs::copy(store.path().unwrap(), &copy).unwrap();
                    let cold = Arc::new(ck_server::testing::config_store(&copy).unwrap());
                    let restored = CredentialDb::initialize(cold.clone()).unwrap();
                    assert_eq!(restored.read(&restored.auth_dir().join(&name)).unwrap().unwrap(), bytes);
                    drop((restored, cold));
                }
                "storage" => {
                    assert!(result.is_err());
                    assert!(error.unwrap().contains("HTTP 500"));
                    assert_eq!(db.read(&target).unwrap().unwrap(), seed);
                }
                "replaced" => {
                    assert!(result.is_err());
                    assert!(error.unwrap().contains("HTTP 412"));
                    assert_eq!(db.read(&target).unwrap().unwrap(), replacement);
                }
                _ => {
                    assert!(result.is_err());
                    assert!(error.unwrap().contains("HTTP 412"));
                    assert!(
                        db.read(&target).unwrap().is_none(),
                        "a returned grant cannot resurrect a deleted source"
                    );
                }
            }
            drop(blocked);
            Arc::try_unwrap(scratch).unwrap().close().unwrap();
        }
        assert_eq!(
            grants.load(Ordering::SeqCst),
            4,
            "one local grant per invocation, no provider retries"
        );
        store
            .edit("test", "SIWC options", |document| {
                document["oauth"]["providers"]["codex"]["signin-with-chatgpt-identifier"] =
                    serde_json::json!("owned-fixture");
                document["requests"]["proxy-url"] =
                    serde_json::json!("http://fixture-user:fixture-password@127.0.0.1:9");
                document["server"]["github-token"] = serde_json::json!("unneeded-server-secret");
                Ok(())
            })
            .unwrap();
        // A built-in login reads only the proxy and the SIWC identifier, never the
        // complete secret-bearing configuration (the compiler adds `config-version`).
        let minimal = client.login_config().await.unwrap();
        let sections: Vec<_> = minimal.document.as_mapping().unwrap().keys().cloned().collect();
        assert!(
            sections
                .iter()
                .all(|key| key == "requests" || key == "oauth" || key == "config-version"),
            "{sections:?}"
        );
        assert!(
            !serde_json::to_string(&minimal.document)
                .unwrap()
                .contains("unneeded-server-secret")
        );
        let siwc_seed = br#"{"type":"codex","access_token":"outer-at","refresh_token":"outer-rt","opaque":{"keep":true},"siwc":{"device_id":"E61BBE28-07EF-466D-8E5D-A344F94AB305"}}"#;
        for case in ["success", "storage", "deleted"] {
            db.save(&target, siwc_seed).unwrap();
            client.credential_admission().await.unwrap();
            let mut config = client.login_config().await.unwrap();
            assert_eq!(ck_exec::codex_siwc::identifier(&config), "owned-fixture");
            assert_eq!(
                config.document["requests"]["proxy-url"].as_str().unwrap(),
                "http://fixture-user:fixture-password@127.0.0.1:9"
            );
            let sources = client.credential_sources().await.unwrap();
            let selected = online_siwc_target(target.to_str().unwrap(), &sources).unwrap();
            let scratch = Arc::new(
                tempfile::Builder::new()
                    .prefix("cooker-login-")
                    .tempdir_in(&root)
                    .unwrap(),
            );
            hydrate_login_source(
                scratch.path(),
                &scratch.path().join(&selected),
                &sources.documents[&selected].bytes,
            )
            .unwrap();
            config.auth_dir = scratch.path().to_owned();
            let (save, failure) = online_login_save(
                scratch.clone(),
                client.clone(),
                sources.documents,
                tokio::runtime::Handle::current(),
            );
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let (show, shown) = tokio::sync::oneshot::channel::<String>();
            let interaction = ck_exec::claude_login::Interaction {
                manual_delay: std::time::Duration::from_secs(3600),
                prompt: None,
                show_url: Box::new(move |url| {
                    let _ = show.send(url.to_owned());
                }),
            };
            let oauth = ck_exec::codex_oauth::CodexOAuth::with_endpoints(
                wreq::Client::builder().no_proxy().build().unwrap(),
                &format!("{base}/mock/token"),
                &format!("{base}/mock/usercode"),
                &format!("{base}/mock/device"),
            )
            .with_siwc_token_url(&format!("{base}/mock/token"));
            let provider = tokio::spawn(async move {
                ck_exec::codex_siwc_bind::bind_with(&config, &selected, port, oauth, interaction, Some(save)).await
            });
            let auth = url::Url::parse(&shown.await.unwrap()).unwrap();
            let query = |key: &str| auth.query_pairs().find(|(name, _)| name == key).unwrap().1.into_owned();
            assert_eq!(query("agent_name_hint"), "owned-fixture");
            assert_eq!(
                query("ext_agent_host_id"),
                "urn:uuid:e61bbe28-07ef-466d-8e5d-a344f94ab305",
                "the captured device ID must exist in scratch before upstream work"
            );
            let blocked = (case == "storage").then(|| ck_server::testing::block_checkpoint_install(&store));
            if case == "deleted" {
                db.delete(&target).unwrap();
            }
            wreq::Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(format!(
                    "http://127.0.0.1:{port}/auth/callback?code=local-code&state={}&client_id=oaiapp_fixture",
                    query("state")
                ))
                .send()
                .await
                .unwrap();
            let result = provider.await.unwrap();
            let error = failure.lock().unwrap().take();
            if case == "success" {
                result.unwrap();
                assert!(error.is_none());
                let bytes = db.read(&target).unwrap().unwrap();
                let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(doc["access_token"], "outer-at");
                assert_eq!(doc["refresh_token"], "outer-rt");
                assert_eq!(doc["opaque"]["keep"], true);
                assert_eq!(doc["use_siwc"], true);
                assert_eq!(doc["siwc"]["client_id"], "oaiapp_fixture");
                assert_eq!(doc["siwc"]["device_id"], "e61bbe28-07ef-466d-8e5d-a344f94ab305");
            } else {
                assert!(result.is_err());
                assert!(
                    error
                        .unwrap()
                        .contains(if case == "storage" { "HTTP 500" } else { "HTTP 412" })
                );
                if case == "storage" {
                    assert_eq!(db.read(&target).unwrap().unwrap(), siwc_seed);
                } else {
                    assert!(db.read(&target).unwrap().is_none());
                }
            }
            drop(blocked);
            Arc::try_unwrap(scratch).unwrap().close().unwrap();
        }
        assert_eq!(
            grants.load(Ordering::SeqCst),
            7,
            "SIWC also consumes only one local grant per invocation"
        );
        for provider in ["kimi", "kimi-ai"] {
            for case in ["success", "storage"] {
                client.credential_admission().await.unwrap();
                let sources = client.credential_sources().await.unwrap();
                let scratch = Arc::new(
                    tempfile::Builder::new()
                        .prefix("cooker-login-")
                        .tempdir_in(&root)
                        .unwrap(),
                );
                let (save, failure) = online_login_save(
                    scratch.clone(),
                    client.clone(),
                    sources.documents,
                    tokio::runtime::Handle::current(),
                );
                let before = db.documents().unwrap();
                let blocked = (case == "storage").then(|| ck_server::testing::block_checkpoint_install(&store));
                let flow = ck_exec::kimi_auth::DeviceFlow::new(
                    wreq::Client::builder().no_proxy().build().unwrap(),
                    "kimi.com",
                    "e61bbe28-07ef-466d-8e5d-a344f94ab305",
                )
                .with_oauth_host(&base);
                let result = ck_exec::kimi_auth::login_with(flow, provider, scratch.path(), true, Some(save)).await;
                let error = failure.lock().unwrap().take();
                if case == "success" {
                    let path = result.unwrap();
                    assert!(error.is_none());
                    let bytes = db
                        .read(&db.auth_dir().join(path.file_name().unwrap()))
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        bytes,
                        std::fs::read(path).unwrap(),
                        "commit exact provider-rendered bytes"
                    );
                    let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(doc["type"], provider);
                    assert_eq!(doc["access_token"], "kimi-FAKE");
                    assert_eq!(doc["refresh_token"], "kimi-FAKE-REFRESH");
                    assert_eq!(db.documents().unwrap().len(), before.len() + 1);
                } else {
                    assert!(result.is_err());
                    assert!(error.unwrap().contains("HTTP 500"));
                    assert_eq!(
                        db.documents().unwrap(),
                        before,
                        "failed new login must not alter earlier credentials"
                    );
                }
                drop(blocked);
                Arc::try_unwrap(scratch).unwrap().close().unwrap();
            }
        }
        assert_eq!(
            kimi_grants.load(Ordering::SeqCst),
            4,
            "one local token exchange per Kimi/Kimi.ai invocation"
        );
        let xai_target = db.auth_dir().join(xai_case["existing_name"].as_str().unwrap());
        let xai_seed = xai_case["existing"].as_str().unwrap().as_bytes();
        for case in ["success", "storage", "replaced"] {
            db.save(&xai_target, xai_seed).unwrap();
            client.credential_admission().await.unwrap();
            let sources = client.credential_sources().await.unwrap();
            let scratch = Arc::new(
                tempfile::Builder::new()
                    .prefix("cooker-login-")
                    .tempdir_in(&root)
                    .unwrap(),
            );
            let (save, failure) = online_login_save(
                scratch.clone(),
                client.clone(),
                sources.documents,
                tokio::runtime::Handle::current(),
            );
            let replacement = br#"{"type":"xai","access_token":"replacement","opaque":"never overwrite"}"#;
            if case == "replaced" {
                db.save(&xai_target, replacement).unwrap();
            }
            let blocked = (case == "storage").then(|| ck_server::testing::block_checkpoint_install(&store));
            let auth = ck_exec::xai_auth::XaiAuth::new(wreq::Client::builder().no_proxy().build().unwrap())
                .with_issuer_origin(&base);
            let result = ck_exec::xai_auth::login_with(auth, scratch.path(), true, Some(save)).await;
            let error = failure.lock().unwrap().take();
            if case == "success" {
                let path = result.unwrap();
                assert!(error.is_none());
                assert_eq!(path.file_name(), xai_target.file_name());
                let bytes = db.read(&xai_target).unwrap().unwrap();
                assert_eq!(bytes, std::fs::read(path).unwrap());
                let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(doc["access_token"], "at-2");
                assert_eq!(doc["note"], "<kept>");
                assert_eq!(doc["prefix"], "team");
                assert_eq!(doc["disabled"], true);
            } else {
                assert!(result.is_err());
                assert!(
                    error
                        .unwrap()
                        .contains(if case == "storage" { "HTTP 500" } else { "HTTP 412" })
                );
                assert_eq!(
                    db.read(&xai_target).unwrap().unwrap(),
                    if case == "storage" { xai_seed } else { replacement }
                );
            }
            drop(blocked);
            Arc::try_unwrap(scratch).unwrap().close().unwrap();
        }
        assert_eq!(xai_grants.load(Ordering::SeqCst), 3);
        // The file the login writes: org-1 hashes to c24787a7 (credential_file_name).
        let existing = db.auth_dir().join("claude-c24787a7-a@example.invalid.json");
        let claude_seed = br#"{"type":"claude","email":"a@example.invalid","organization_uuid":"org-1","access_token":"old","note":"<kept>","proxy_url":"socks5://keep:1"}"#;
        let mut canonical: Option<PathBuf> = None;
        for case in ["success", "storage", "replaced"] {
            if let Some(path) = canonical.take() {
                db.delete(&path).unwrap();
            }
            db.save(&existing, claude_seed).unwrap();
            client.credential_admission().await.unwrap();
            let sources = client.credential_sources().await.unwrap();
            let scratch = Arc::new(
                tempfile::Builder::new()
                    .prefix("cooker-login-")
                    .tempdir_in(&root)
                    .unwrap(),
            );
            hydrate_claude_sources(scratch.path(), &sources.documents).unwrap();
            let (save, failure) = online_login_save(
                scratch.clone(),
                client.clone(),
                sources.documents,
                tokio::runtime::Handle::current(),
            );
            if case == "replaced" {
                db.save(
                    &existing,
                    br#"{"type":"claude","access_token":"replacement","note":"never delete"}"#,
                )
                .unwrap();
            }
            let before = db.documents().unwrap();
            let blocked = (case == "storage").then(|| ck_server::testing::block_checkpoint_install(&store));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (shown, url) = tokio::sync::oneshot::channel::<String>();
            let interaction = ck_exec::claude_login::Interaction {
                manual_delay: std::time::Duration::from_secs(3600),
                prompt: None,
                show_url: Box::new(move |url| {
                    let _ = shown.send(url.to_owned());
                }),
            };
            let oauth = ck_exec::oauth::OAuth::with_endpoints(
                wreq::Client::builder().no_proxy().build().unwrap(),
                &format!("{base}/mock/claude/token"),
                &format!("{base}/mock/claude/profile"),
                &format!("{base}/mock/claude/roles"),
            );
            let owner = scratch.clone();
            let provider = tokio::spawn(async move {
                ck_exec::claude_login::login_with(owner.path(), listener, oauth, interaction, Some(save)).await
            });
            let auth = url::Url::parse(&url.await.unwrap()).unwrap();
            let state = auth
                .query_pairs()
                .find(|(key, _)| key == "state")
                .unwrap()
                .1
                .into_owned();
            wreq::Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(format!("http://127.0.0.1:{port}/callback?code=fake-code&state={state}"))
                .send()
                .await
                .unwrap();
            let result = provider.await.unwrap();
            let error = failure.lock().unwrap().take();
            if case == "success" {
                let path = result.unwrap();
                assert!(error.is_none());
                let target = db.auth_dir().join(path.file_name().unwrap());
                assert_eq!(target, existing);
                let bytes = db.read(&target).unwrap().unwrap();
                assert_eq!(bytes, std::fs::read(path).unwrap());
                let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(doc["access_token"], "sk-ant-oat-login-fake");
                assert_eq!(doc["note"], "<kept>");
                assert_eq!(doc["proxy_url"], "socks5://keep:1");
                canonical = Some(target);
            } else {
                assert!(result.is_err());
                assert!(
                    error
                        .unwrap()
                        .contains(if case == "storage" { "HTTP 500" } else { "HTTP 412" })
                );
                assert_eq!(
                    db.documents().unwrap(),
                    before,
                    "a failed login must not partially commit"
                );
            }
            drop(blocked);
            Arc::try_unwrap(scratch).unwrap().close().unwrap();
        }
        assert_eq!(claude_grants.load(Ordering::SeqCst), 3);
        let guarded = db.auth_dir().join("guarded.json");
        db.save(&guarded, claude_seed).unwrap();
        let sources = client.credential_sources().await.unwrap();
        let scratch = Arc::new(
            tempfile::Builder::new()
                .prefix("cooker-login-")
                .tempdir_in(&root)
                .unwrap(),
        );
        let (save, failure) = online_login_save(
            scratch.clone(),
            client.clone(),
            sources.documents,
            tokio::runtime::Handle::current(),
        );
        let target = scratch.path().join("guarded.json");
        let failed_read = tokio::task::spawn_blocking(move || {
            ck_exec::login_store::save_login(Some(&save), std::slice::from_ref(&target), || {
                std::fs::remove_file(&target).map_err(|error| error.to_string())?;
                std::fs::create_dir(&target).map_err(|error| error.to_string())
            })
        })
        .await
        .unwrap();
        assert!(failed_read.is_err(), "a rendered read error is not deletion intent");
        assert!(failure.lock().unwrap().take().is_some());
        assert_eq!(db.read(&guarded).unwrap().unwrap(), claude_seed);
        Arc::try_unwrap(scratch).unwrap().close().unwrap();
        let sources = client.credential_sources().await.unwrap();
        let scratch = Arc::new(
            tempfile::Builder::new()
                .prefix("cooker-login-")
                .tempdir_in(&root)
                .unwrap(),
        );
        let (save, failure) = online_login_save(
            scratch.clone(),
            client.clone(),
            sources.documents,
            tokio::runtime::Handle::current(),
        );
        let repeat = db.auth_dir().join("repeat-opaque.json");
        for value in [1, 2, 3] {
            if value == 3 {
                db.save(&repeat, br#"{"type":"opaque-type","opaque":"external replacement"}"#)
                    .unwrap();
            }
            let path = scratch.path().join("repeat-opaque.json");
            let callback = save.clone();
            let bytes = serde_json::to_vec(&serde_json::json!({"type":"opaque-type","opaque":value})).unwrap();
            let rendered = bytes.clone();
            let result = tokio::task::spawn_blocking(move || {
                ck_exec::login_store::save_login(Some(&callback), std::slice::from_ref(&path), || {
                    std::fs::write(&path, rendered).map_err(|error| error.to_string())
                })
            })
            .await
            .unwrap();
            if value < 3 {
                result.unwrap();
                assert_eq!(
                    db.read(&repeat).unwrap().unwrap(),
                    bytes,
                    "a later host save may build only on its own acknowledged result"
                );
            } else {
                assert!(result.is_err());
                assert!(failure.lock().unwrap().take().unwrap().contains("HTTP 412"));
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&db.read(&repeat).unwrap().unwrap()).unwrap()["opaque"],
                    "external replacement"
                );
            }
        }
        drop(save);
        Arc::try_unwrap(scratch).unwrap().close().unwrap();
        let sources = client.credential_sources().await.unwrap();
        let scratch = Arc::new(
            tempfile::Builder::new()
                .prefix("cooker-login-")
                .tempdir_in(&root)
                .unwrap(),
        );
        let (save, failure) = online_login_save(
            scratch.clone(),
            client.clone(),
            sources.documents,
            tokio::runtime::Handle::current(),
        );
        let a_entered = Arc::new(tokio::sync::Notify::new());
        let b_entered = Arc::new(tokio::sync::Notify::new());
        let (release_a, wait_a) = std::sync::mpsc::channel::<()>();
        let (release_b, wait_b) = std::sync::mpsc::channel::<()>();
        let path = scratch.path().join("parallel-opaque.json");
        let a_bytes = br#"{"type":"opaque-type","invocation":"A"}"#.to_vec();
        let b_bytes = br#"{"type":"opaque-type","invocation":"B"}"#.to_vec();
        let (callback, target, started, rendered) = (save.clone(), path.clone(), a_entered.clone(), a_bytes.clone());
        let a = tokio::task::spawn_blocking(move || {
            ck_exec::login_store::save_login(Some(&callback), std::slice::from_ref(&target), || {
                std::fs::write(&target, rendered).map_err(|error| error.to_string())?;
                started.notify_one();
                wait_a.recv().map_err(|error| error.to_string())
            })
        });
        a_entered.notified().await;
        let (callback, target, started, rendered) = (save.clone(), path, b_entered.clone(), b_bytes);
        let mut b = tokio::task::spawn_blocking(move || {
            ck_exec::login_store::save_login(Some(&callback), std::slice::from_ref(&target), || {
                std::fs::write(&target, rendered).map_err(|error| error.to_string())?;
                started.notify_one();
                wait_b.recv().map_err(|error| error.to_string())
            })
        });
        let early_b = tokio::select! {
            _ = b_entered.notified() => None,
            result = &mut b => Some(result.unwrap()),
        };
        release_a.send(()).unwrap();
        let a_result = a.await.unwrap();
        let b_result = match early_b {
            Some(result) => result,
            None => {
                release_b.send(()).unwrap();
                b.await.unwrap()
            }
        };
        a_result.unwrap();
        assert_eq!(
            db.read(&db.auth_dir().join("parallel-opaque.json")).unwrap().unwrap(),
            a_bytes,
            "a successful callback must acknowledge its own rendered output, never an overlapping writer's bytes"
        );
        assert!(
            b_result.is_err(),
            "overlap must conflict or serialize, not replace an in-flight renderer"
        );
        assert!(failure.lock().unwrap().take().is_some());
        drop(save);
        Arc::try_unwrap(scratch).unwrap().close().unwrap();
        server.abort();
        let _ = server.await;
        drop((client, state, rt, db, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mixed_command_flags_use_the_same_priority_for_online_admission_and_execution() {
        for (flags, selected, online) in [
            (
                vec!["--vertex-import", "key.json", "--siwc-bind", "target.json"],
                BuiltinCommand::Vertex,
                true,
            ),
            (
                vec!["--codex-login", "--siwc-bind", "target.json"],
                BuiltinCommand::Siwc,
                true,
            ),
            (
                vec!["--codex-device-login", "--antigravity-login"],
                BuiltinCommand::Antigravity,
                true,
            ),
            (vec!["--codex-login", "--claude-login"], BuiltinCommand::Codex, true),
            (vec!["--claude-login", "--kimi-login"], BuiltinCommand::Claude, true),
        ] {
            let args = Args::try_parse_from(std::iter::once("cooker").chain(flags.iter().copied())).unwrap();
            assert_eq!(selected_command(&args), Some(selected), "{flags:?}");
            assert_eq!(online_command_supported(&args), online, "{flags:?}");
        }
    }

    #[test]
    fn online_siwc_destinations_keep_owner_identity_without_client_filesystem_access() {
        let root = credential_test_dir().join("owner-cache");
        assert!(!root.exists());
        let sources = tui::CredentialSources {
            auth_dir: root.clone(),
            documents: std::collections::BTreeMap::from([(
                "c.json".into(),
                tui::CredentialSource {
                    bytes: Vec::new(),
                    sha256: "00".repeat(32),
                },
            )]),
        };
        assert_eq!(online_siwc_target(" c.json ", &sources).unwrap(), "c.json");
        assert_eq!(
            online_siwc_target(root.join("c.json").to_str().unwrap(), &sources).unwrap(),
            "c.json"
        );
        for target in ["../c.json", "nested/c.json", "missing.json", "."] {
            assert!(online_siwc_target(target, &sources).is_err(), "{target}");
        }
        assert!(online_siwc_target(root.parent().unwrap().join("outside.json").to_str().unwrap(), &sources).is_err());
        assert!(
            !root.exists(),
            "resolving a remote destination must not create or inspect a client cache"
        );
    }

    #[test]
    fn cli_changes_and_deletions_survive_reopen() {
        let root = credential_test_dir();
        let path = root.join("cooker.db");
        let mut config = Config::parse("").unwrap();
        let db =
            initialize_credentials(Arc::new(ck_server::testing::config_store(&path).unwrap()), &mut config).unwrap();
        assert_eq!(config.auth_dir, db.auth_dir());
        db.save(
            &config.auth_dir.join("claude-old.json"),
            b"{\"type\":\"claude\",\"old\":true}",
        )
        .unwrap();
        let canonical = config.auth_dir.join("claude-new.json");
        let bytes = b"{\"type\":\"claude\",\"access_token\":\"new\",\"unknown\":[1,2]}";
        let (save, failure) = login_save(db.clone());
        ck_exec::login_store::save_login(
            Some(&save),
            &[canonical.clone(), config.auth_dir.join("claude-old.json")],
            || {
                std::fs::write(&canonical, bytes).map_err(|error| error.to_string())?;
                std::fs::remove_file(config.auth_dir.join("claude-old.json")).map_err(|error| error.to_string())
            },
        )
        .unwrap();
        assert!(failure.lock().unwrap().is_none());
        drop(save);
        drop(db);
        std::fs::remove_dir_all(&config.auth_dir).unwrap();
        let db = CredentialDb::initialize(Arc::new(ck_server::testing::config_store(&path).unwrap())).unwrap();
        assert_eq!(std::fs::read(&canonical).unwrap(), bytes);
        assert!(db.read(&config.auth_dir.join("claude-old.json")).unwrap().is_none());
        assert!(!config.auth_dir.join("claude-old.json").exists());
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cli_durable_failure_is_returned_and_restores_working_bytes() {
        let root = credential_test_dir();
        let path = root.join("cooker.db");
        let owner = Arc::new(ck_server::testing::config_store(&path).unwrap());
        let db = CredentialDb::initialize(owner.clone()).unwrap();
        let auth = db.auth_dir().join("login.json");
        db.save(&auth, b"{\"old\":true}").unwrap();
        let (save, failure) = login_save(db.clone());
        let blocker = ck_server::testing::block_checkpoint_install(&owner);
        let mut called = false;
        let result = ck_exec::login_store::save_login(Some(&save), std::slice::from_ref(&auth), || {
            called = true;
            std::fs::write(&auth, b"{\"new\":true}").map_err(|error| error.to_string())
        });
        drop(blocker);
        assert!(result.unwrap_err().contains("commit CLI credentials to SQLite"));
        assert!(failure.lock().unwrap().is_some());
        assert!(
            called,
            "a real writer mutation must be rolled back after failed checkpoint installation"
        );
        assert_eq!(std::fs::read(&auth).unwrap(), b"{\"old\":true}");
        assert_eq!(db.read(&auth).unwrap().unwrap(), b"{\"old\":true}");
        drop((save, db, owner));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cli_writer_cannot_be_reverted_by_concurrent_materialization() {
        let root = credential_test_dir();
        let path = root.join("cooker.db");
        let db = CredentialDb::initialize(Arc::new(ck_server::testing::config_store(&path).unwrap())).unwrap();
        let auth = db.auth_dir().join("login.json");
        db.save(&auth, b"{\"old\":true}").unwrap();
        let watcher = db.clone();
        let (save, _) = login_save(db.clone());
        let (written, changed) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let target = auth.clone();
        let writer = std::thread::spawn(move || {
            ck_exec::login_store::save_login(Some(&save), std::slice::from_ref(&target), || {
                std::fs::write(&target, b"{\"new\":true}").map_err(|error| error.to_string())?;
                written.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
            .unwrap()
        });
        changed.recv().unwrap();
        let (started, starting) = std::sync::mpsc::channel();
        let (finished, done) = std::sync::mpsc::channel();
        let materializer = std::thread::spawn(move || {
            started.send(()).unwrap();
            watcher.materialize().unwrap();
            finished.send(()).unwrap();
        });
        starting.recv().unwrap();
        let blocked = done.recv_timeout(Duration::from_millis(100));
        release.send(()).unwrap();
        writer.join().unwrap();
        materializer.join().unwrap();
        assert!(matches!(blocked, Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(db.read(&auth).unwrap().unwrap(), b"{\"new\":true}");
        assert_eq!(std::fs::read(&auth).unwrap(), b"{\"new\":true}");
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Positive configured worker counts override the CPU-based default.
    #[test]
    fn worker_threads_come_from_the_config() {
        let default = std::thread::available_parallelism().map_or(1, |n| n.get().min(2));
        let cfg = Config::parse("worker-threads: 5\n").unwrap();
        assert_eq!(worker_threads(Some(&cfg)), 5);
        for text in ["worker-threads: 0\n", "worker-threads: -3\n", ""] {
            let cfg = Config::parse(text).unwrap();
            assert_eq!(worker_threads(Some(&cfg)), default, "{text:?}");
        }
        assert_eq!(worker_threads(None), default);
    }

    // Expected values: Go resolveManagementBaseURL (tests/fixtures/discovery_main_go.json).
    #[test]
    fn management_base_url_follows_go() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/discovery_main_go.json")).unwrap();
        let cases = fixture["management_base_url"].as_array().unwrap();
        assert!(cases.len() >= 6);
        for case in cases {
            let got = management_base_url(
                case["flag"].as_str().unwrap(),
                case["remote"].as_str().unwrap(),
                case["port"].as_i64().unwrap(),
            );
            assert_eq!(got, case["out"].as_str().unwrap(), "{case}");
        }
        let config = Config::parse("config-version: 8\nmanagement:\n  base-url: ' http://y '\n").unwrap();
        assert_eq!(resolve_management_base_url("", &config), "http://y");
    }

    #[test]
    fn standalone_url_follows_the_listener() {
        let url = |addr: &str, tls| standalone_base_url((addr.parse().unwrap(), tls));
        assert_eq!(url("127.0.0.1:4100", false).unwrap(), "http://127.0.0.1:4100");
        assert_eq!(url("0.0.0.0:4101", false).unwrap(), "http://127.0.0.1:4101");
        assert_eq!(url("[::]:4102", false).unwrap(), "http://[::1]:4102");
        assert_eq!(url("[::1]:4103", false).unwrap(), "http://[::1]:4103");
        assert!(url("192.0.2.7:4104", false).unwrap_err().contains("192.0.2.7"));
        assert!(url("127.0.0.1:4105", true).unwrap_err().contains("server.tls"));
    }

    /// `--tui --standalone` with no management key anywhere and `port: 0`: the TUI finds
    /// the ephemeral port and signs in with the generated password.
    #[tokio::test]
    async fn service_shutdown_closes_keep_alive_and_settles_its_writer_before_cleanup() {
        shutdown_driver(false).await;
    }

    #[tokio::test]
    async fn service_shutdown_reports_unpublished_checkpoint_and_resumes_it() {
        shutdown_driver(true).await;
    }

    async fn shutdown_driver(publication_unavailable: bool) {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        struct Release(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for Release {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        async fn response(stream: &mut tokio::net::TcpStream) -> u16 {
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(stream.read_u8().await.unwrap());
                assert!(head.len() < 16384);
            }
            let head = std::str::from_utf8(&head).unwrap();
            let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
            let length: usize = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            status
        }
        let root = credential_test_dir();
        let store = Arc::new(ck_server::testing::config_store(&root.join("service.db")).unwrap());
        store
            .import_yaml("server:\n  host: 127.0.0.1\n  port: 0\n", "test", "shutdown driver")
            .unwrap();
        let mut config = store.load().unwrap().config;
        let backend = initialize_credentials(store.clone(), &mut config).unwrap();
        let workspace = ck_server::testing::working_directory(&store).unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = std::sync::Mutex::new(Some(entered));
        let (send, held) = std::sync::mpsc::channel();
        let mut release = Some(Release(Some(send)));
        let held = std::sync::Mutex::new(held);
        let marker = workspace.join("settled-driver-writer");
        let observer = store
            .subscribe_configuration({
                let armed = armed.clone();
                move |_| {
                    if armed.swap(false, Ordering::SeqCst) {
                        entered.lock().unwrap().take().unwrap().send(()).unwrap();
                        held.lock().unwrap().recv().unwrap();
                        std::fs::write(&marker, b"writer finished before cleanup")?;
                    }
                    Ok(())
                }
            })
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (listening, listener) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(
            config,
            store.clone(),
            backend,
            "fake-driver-password".into(),
            true,
            async {
                let _ = stopped.await;
            },
            Some(listening),
        ));
        let (address, tls) = listener.await.unwrap();
        assert!(!tls);
        let mut keep_alive = tokio::net::TcpStream::connect(address).await.unwrap();
        keep_alive
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), response(&mut keep_alive))
                .await
                .unwrap(),
            200
        );
        armed.store(true, Ordering::SeqCst);
        let mut writing = tokio::spawn(async move {
            wreq::Client::builder().no_proxy().build().unwrap()
                .post(format!("http://{address}/admin/v1/apply"))
                .bearer_auth("fake-driver-password").json(&serde_json::json!({"ops":[{"op":"put","kind":"setting","id":"routing","spec":{"retry":{"request-retry":9}}}]}))
                .send().await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                result = started => result.unwrap(),
                result = &mut writing => {
                    let response = result.unwrap().unwrap();
                    panic!("writer returned before the admission barrier: {} {}", response.status(), response.text().await.unwrap());
                },
            }
        }).await.unwrap();
        drop(store);
        if publication_unavailable {
            drop(release.take());
            assert!((&mut writing).await.unwrap().unwrap().status().is_success());
            std::fs::rename(root.join("service.db"), root.join("old.db")).unwrap();
            std::fs::create_dir(root.join("service.db")).unwrap();
            let failed = wreq::Client::builder().no_proxy().build().unwrap()
                .post(format!("http://{address}/admin/v1/apply")).bearer_auth("fake-driver-password")
                .json(&serde_json::json!({"ops":[{"op":"put","kind":"setting","id":"routing","spec":{"retry":{"request-retry":11}}}]}))
                .send().await.unwrap();
            assert_eq!(failed.status().as_u16(), 500);
        }
        stop.send(()).unwrap();
        let mut byte = [0];
        let closed = tokio::time::timeout(std::time::Duration::from_secs(10), keep_alive.read(&mut byte))
            .await
            .expect("shutdown must close an established inference keep-alive connection");
        assert!(
            matches!(closed, Ok(0)) || closed.is_err(),
            "stopped transport still served the connection"
        );
        if !publication_unavailable {
            assert!(
                !server.is_finished(),
                "driver returned before its admitted writer settled"
            );
            assert!(workspace.is_dir());
        }
        drop(release.take());
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(10), server)
            .await
            .unwrap()
            .unwrap();
        drop(observer);
        let restored = if publication_unavailable {
            assert!(stopped.unwrap_err().to_string().contains("incomplete"));
            assert!(workspace.is_dir(), "failed driver erased the only newer checkpoint");
            std::fs::remove_dir(root.join("service.db")).unwrap();
            std::fs::rename(root.join("old.db"), root.join("service.db")).unwrap();
            ck_server::testing::config_store(&root.join("service.db")).unwrap()
        } else {
            stopped.unwrap();
            let _ = writing.await.unwrap();
            assert!(!workspace.exists(), "driver left owned work alive after return");
            std::fs::copy(root.join("service.db"), root.join("restored.db")).unwrap();
            ck_server::testing::config_store(&root.join("restored.db")).unwrap()
        };
        assert_eq!(
            restored.load().unwrap().config.routing.retry.request_retry,
            if publication_unavailable { 11 } else { 9 }
        );
        drop(restored);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn standalone_server_starts_without_any_secret() {
        let dir = std::env::temp_dir().join(format!("cooker-standalone-{}", std::process::id()));
        let auth = dir.join("auth");
        std::fs::create_dir_all(&auth).unwrap();
        let store = Arc::new(ck_server::testing::config_store(&dir.join("cooker.db")).unwrap());
        store
            .import_yaml("server:\n  host: 127.0.0.1\n  port: 0\n", "test", "standalone server")
            .unwrap();
        let mut config = store.load().unwrap().config;
        let backend = initialize_credentials(store.clone(), &mut config).unwrap();
        assert!(config.management.secret_key.is_empty());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (listening, listener) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(
            config,
            store,
            backend,
            "fake-tui-password".into(),
            true,
            async {
                let _ = stopped.await;
            },
            Some(listening),
        ));
        let base = standalone_base_url(listener.await.unwrap()).unwrap();
        assert_ne!(base, "http://127.0.0.1:0");
        assert!(tui::wait_ready(&base, "fake-tui-password").await);
        let _ = stop.send(());
        server.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestSIWCManagementRouteAccessControl`: the v8 SIWC bind route answers its
    /// own validation once authorized, 401 without the key, and 403 naming
    /// CK_MANAGEMENT_PASSWORD when no key exists (Go answers an empty 404; changed on
    /// purpose, owner ruling 2026-11-13).
    #[tokio::test]
    async fn siwc_bind_route_follows_management_availability() {
        use tower::ServiceExt as _;
        let dir = std::env::temp_dir().join(format!("cooker-siwc-route-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("auth")).unwrap();
        let store = Arc::new(ConfigStore::memory_with(""));
        let config = store.load().unwrap().config;
        for (name, enabled, authorized, want) in [
            ("authorized invalid target", true, true, 400),
            ("missing management key", true, false, 401),
            ("management disabled", false, true, 403),
        ] {
            let executors = Executors {
                claude: ClaudeExecutor::new("http://127.0.0.1:9").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            };
            let rt = Arc::new(ck_server::testing::runtime(config.clone(), Vec::new(), executors));
            let options = ck_server::management::api::Options {
                // CK_MANAGEMENT_PASSWORD; with no config secret it alone decides availability.
                management_password: Some(if enabled {
                    "fake-management-only".into()
                } else {
                    String::new()
                }),
                // Keeps the log-folder probe out of the working directory.
                log_dir: Some(dir.join("logs")),
                ..Default::default()
            };
            let management = ck_server::management::api::Management::with_options(rt.clone(), store.clone(), options);
            let mut request = axum::http::Request::post("/v8/management/oauth/siwc/bind")
                .header("content-type", "application/json")
                .extension(axum::extract::ConnectInfo(SocketAddr::from((
                    Ipv4Addr::LOCALHOST,
                    1234,
                ))));
            if authorized {
                request = request.header("authorization", "Bearer fake-management-only");
            }
            let response = app(&rt, &management)
                .oneshot(request.body(axum::body::Body::from("{}")).unwrap())
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(status, want, "{name}: {}", String::from_utf8_lossy(&body));
            match want {
                400 => assert_eq!(&body[..], br#"{"error":"name is required"}"#, "{name}"),
                403 => assert_eq!(
                    &body[..],
                    br#"{"error":"management key not set: start the server with CK_MANAGEMENT_PASSWORD"}"#,
                    "{name}"
                ),
                _ => {}
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn optional_first_import_only_tolerates_a_missing_file() {
        let dir = std::env::temp_dir().join(format!(
            "cooker-cloud-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("cooker.db");
        let missing = dir.join("missing.yaml");
        let (store, cfg) = load_config(&db, &missing, FirstImport::Cloud).unwrap();
        assert_eq!(cfg.port, 0);
        assert!(!store.is_empty().unwrap(), "even an empty initial import is durable");
        assert!(!cloud_config_present(&store, &cfg));
        std::fs::write(&missing, "server:\n  port: 9000\n").unwrap();
        drop(store);
        assert_eq!(load_config(&db, &missing, FirstImport::Required).unwrap().1.port, 0);
        for (i, text) in ["server:\n  port: [\n", "server:\n  port: wrong\n", "- a\n- b\n"]
            .iter()
            .enumerate()
        {
            let broken = dir.join("broken.yaml");
            std::fs::write(&broken, text).unwrap();
            assert!(
                load_config(&dir.join(format!("invalid-{i}.db")), &broken, FirstImport::Cloud).is_err(),
                "{text}"
            );
        }
        assert!(load_config(&dir.join("directory.db"), &dir, FirstImport::Cloud).is_err());
        let invalid_db = dir.join("invalid.db");
        std::fs::write(&invalid_db, "not SQLite").unwrap();
        assert!(
            load_config(&invalid_db, &missing, FirstImport::Cloud).is_err(),
            "a valid YAML file cannot mask a bad DB"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A fresh volume: no database and no ./config.yaml start with defaults on port 8317;
    /// an explicit `--config` that is missing still fails and creates no database.
    #[test]
    fn a_fresh_volume_starts_with_defaults_on_the_default_port() {
        let dir = std::env::temp_dir().join(format!(
            "cooker-fresh-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("config.yaml");
        let required = dir.join("required.db");
        assert!(load_config(&required, &missing, FirstImport::Required).is_err());
        assert!(!required.exists(), "a failed required import creates no database");
        let db = dir.join("cooker.db");
        let (store, cfg) = load_config(&db, &missing, FirstImport::Defaults).unwrap();
        assert_eq!(cfg.port, ck_core::config::DEFAULT_PORT);
        assert!(!store.is_empty().unwrap(), "the defaults are the first revision");
        drop(store);
        std::fs::write(&missing, "server:\n  port: 9\n").unwrap();
        assert_eq!(
            load_config(&db, &missing, FirstImport::Defaults).unwrap().1.port,
            ck_core::config::DEFAULT_PORT,
            "later starts do not read the file"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A blank default `./config.yaml` (an empty or whitespace-only file) starts like a missing
    /// one, on the default port, instead of importing a config that listens on port 0.
    #[test]
    fn a_blank_default_config_starts_with_defaults_on_the_default_port() {
        let dir = std::env::temp_dir().join(format!(
            "cooker-blank-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.yaml");
        for (i, blank) in ["", "  \n\t\n"].into_iter().enumerate() {
            std::fs::write(&file, blank).unwrap();
            let db = dir.join(format!("blank-{i}.db"));
            let (store, cfg) = load_config(&db, &file, FirstImport::Defaults).unwrap();
            assert_eq!(cfg.port, ck_core::config::DEFAULT_PORT, "blank file {i}");
            assert!(!store.is_empty().unwrap(), "the defaults are the first revision");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_existing_database_does_not_reread_the_import_file() {
        let dir = std::env::temp_dir().join(format!(
            "cooker-import-once-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("cooker.db");
        let file = dir.join("config.yaml");
        std::fs::write(&file, "server:\n  port: 9\n").unwrap();
        assert_eq!(load_config(&db, &file, FirstImport::Required).unwrap().1.port, 9);
        std::fs::write(&file, "server:\n  port: 10\n").unwrap();
        assert_eq!(load_config(&db, &file, FirstImport::Required).unwrap().1.port, 9);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn explicit_host_listener_is_nonblocking_for_tokio() {
        let listener = bind("127.0.0.1", 0).unwrap();
        let port = listener.local_addr().unwrap().port();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let client = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port));
        let (client, accepted) = tokio::join!(client, listener.accept());
        assert!(client.is_ok() && accepted.is_ok());
    }

    #[test]
    fn empty_host_is_dual_stack_and_port_in_use_is_an_error() {
        let listener = bind("", 0).unwrap();
        let port = listener.local_addr().unwrap().port();
        if listener.local_addr().unwrap().is_ipv6() {
            std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port)).expect("IPv4 reaches the [::] listener");
            std::net::TcpStream::connect((Ipv6Addr::LOCALHOST, port)).expect("IPv6 reaches the [::] listener");
        }
        let err = bind("", port).unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::AddrInUse,
            "must not silently fall back to IPv4"
        );
    }
}
