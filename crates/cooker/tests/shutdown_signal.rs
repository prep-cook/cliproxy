//! SIGTERM in the real binary logs one stable line and exits 0. The container gate
//! relies on that line to tell a stop from a server that exited on its own. Uses
//! cloud-deploy standby, so no listener or network starts.
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn sigterm_logs_the_received_signal_and_exits_cleanly() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("cooker-sigterm-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_cooker"))
        .args(["--config", "config.yaml"])
        .current_dir(&root)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", &root)
        .env("CK_DEPLOY", "cloud")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut seen = Vec::new();
    for line in lines.by_ref() {
        let line = line.unwrap();
        let standing_by = line.contains("standing by for configuration");
        seen.push(line);
        if standing_by {
            break;
        }
    }
    assert!(seen.last().is_some_and(|line| line.contains("standing by")), "{seen:?}");
    // The handler installs right after the standby line is logged.
    std::thread::sleep(Duration::from_millis(300));
    let kill = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(kill.success());
    seen.extend(lines.map(Result::unwrap));
    let status = child.wait().unwrap();
    assert!(status.success(), "{status:?} {seen:?}");
    assert_eq!(
        seen.iter()
            .filter(|line| line.contains("shutdown signal received: SIGTERM"))
            .count(),
        1,
        "{seen:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}
