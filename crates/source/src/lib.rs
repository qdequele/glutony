//! # meili-ingest source
//!
//! Scheduled sources: turning a location into items, and the two security primitives
//! that make that safe (sealed secrets, an SSRF guard).
//!
//! This crate deliberately depends on neither the gateway, the control plane nor the
//! worker, so every part of it is unit-testable without a server. Blob staging lives in
//! the worker activity, not here — which is what keeps the connector tests free of an
//! object store.
//!
//! See `docs/superpowers/specs/2026-09-13-scheduled-sources-design.md`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod guard;
pub mod secret;

pub use guard::{AddressClass, UrlGuard, check_scheme, classify};
pub use secret::{SecretKey, open_json, seal_json};

/// Errors produced while handling a source.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// `SOURCE_SECRET_KEY` is missing, malformed, or not 32 bytes.
    #[error("source secret key: {0}")]
    Key(String),
    /// Sealing or opening failed (wrong key, truncated or tampered ciphertext).
    #[error("sealed value: {0}")]
    Seal(String),
    /// A sealed payload did not deserialize into the expected type.
    #[error("sealed payload: {0}")]
    Payload(String),
    /// The URL was rejected before any socket was opened (scheme or address policy).
    #[error("blocked url: {0}")]
    Blocked(String),
    /// DNS resolution failed.
    #[error("dns: {0}")]
    Dns(String),
}
