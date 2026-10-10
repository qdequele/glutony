//! The control plane validates its whole configuration before it touches Postgres: a
//! configuration it would refuse must not apply migrations first (an older image
//! could no longer start on that database). No database needed.

use std::process::Command;
use std::time::{Duration, Instant};

/// Run the binary with only `env` set and return its stderr + stdout.
fn boot(env: &[(&str, &str)]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_meili-ingest-control-plane"));
    cmd.env_clear();
    for (k, v) in env {
        cmd.env(k, v);
    }
    let started = Instant::now();
    let out = cmd.output().expect("the binary runs");
    assert!(!out.status.success(), "boot should be refused");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "boot took {:?}",
        started.elapsed()
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    )
}

// Nothing listens on port 1: if the binary connected first, the error would be the
// connection failure, not the configuration one.
const UNREACHABLE_DB: &str = "postgres://postgres:x@127.0.0.1:1/postgres";

#[test]
fn a_legacy_lab_secret_is_refused_before_connecting_to_postgres() {
    let out = boot(&[
        ("DATABASE_URL", UNREACHABLE_DB),
        ("CONTROL_PLANE_TOKEN", "a-real-token"),
        ("LAB_URL", "https://lab.example"),
        ("LAB_INSTANCE_ID", "id"),
        ("LAB_INSTANCE_SECRET", "s"),
        ("LAB_EVENTS_SECRET", "leftover"),
    ]);
    assert!(out.contains("LAB_EVENTS_SECRET"), "{out}");
    assert!(!out.contains("connecting to Postgres"), "{out}");
}

#[test]
fn incomplete_lab_credentials_are_refused_before_connecting_to_postgres() {
    let out = boot(&[
        ("DATABASE_URL", UNREACHABLE_DB),
        ("CONTROL_PLANE_TOKEN", "a-real-token"),
        ("LAB_URL", "https://lab.example"),
    ]);
    assert!(out.contains("LAB_INSTANCE_ID"), "{out}");
    assert!(!out.contains("connecting to Postgres"), "{out}");
}

#[test]
fn a_missing_control_plane_token_is_refused_before_connecting_to_postgres() {
    let out = boot(&[("DATABASE_URL", UNREACHABLE_DB)]);
    assert!(out.contains("CONTROL_PLANE_TOKEN"), "{out}");
    assert!(!out.contains("connecting to Postgres"), "{out}");
}

#[test]
fn a_bad_bind_address_is_refused_before_connecting_to_postgres() {
    let out = boot(&[
        ("DATABASE_URL", UNREACHABLE_DB),
        ("CONTROL_PLANE_TOKEN", "a-real-token"),
        ("BIND", "not an address"),
    ]);
    assert!(out.contains("BIND"), "{out}");
    assert!(!out.contains("connecting to Postgres"), "{out}");
}
