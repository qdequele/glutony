//! Authenticated encryption for the two secrets a source stores: its fetch credential
//! and its `MeiliContext`.
//!
//! Layout of a sealed value: `version(1) ‖ nonce(12) ‖ ciphertext`. The version byte
//! lets a rotation scheme be added later without a migration.

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::SourceError;

/// Current sealed-value format.
const VERSION: u8 = 1;
/// ChaCha20-Poly1305 nonce length.
const NONCE_LEN: usize = 12;

/// Symmetric key used to seal and open a source's secrets.
///
/// Deliberately implements `Debug` as a redaction and derives nothing else, so the key
/// cannot be printed into a log or serialized by accident.
pub struct SecretKey(Key);

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(<redacted>)")
    }
}

impl SecretKey {
    /// Key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Key::from(bytes))
    }

    /// Read `SOURCE_SECRET_KEY` (32 bytes, base64). `Ok(None)` when it is unset, which
    /// callers must turn into a 501: storing a tenant's write key in plaintext because
    /// an env var was missed is not an acceptable degraded mode.
    pub fn from_env() -> Result<Option<Self>, SourceError> {
        let Ok(raw) = std::env::var("SOURCE_SECRET_KEY") else {
            return Ok(None);
        };
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(raw.trim())
            .map_err(|e| SourceError::Key(format!("not valid base64: {e}")))?;
        let bytes: [u8; 32] = decoded.try_into().map_err(|v: Vec<u8>| {
            SourceError::Key(format!("expected 32 bytes, got {}", v.len()))
        })?;
        Ok(Some(Self::from_bytes(bytes)))
    }

    /// Seal `plaintext`. A fresh random nonce is used on every call, so sealing the same
    /// value twice never produces the same ciphertext.
    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SourceError> {
        let cipher = ChaCha20Poly1305::new(&self.0);
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| SourceError::Seal("encryption failed".into()))?;
        let mut out = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        out.push(VERSION);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Open a value produced by [`SecretKey::seal`].
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, SourceError> {
        if sealed.len() <= 1 + NONCE_LEN {
            return Err(SourceError::Seal("sealed value is too short".into()));
        }
        if sealed[0] != VERSION {
            return Err(SourceError::Seal(format!(
                "unsupported sealed format version {}",
                sealed[0]
            )));
        }
        let nonce = Nonce::from_slice(&sealed[1..1 + NONCE_LEN]);
        ChaCha20Poly1305::new(&self.0)
            .decrypt(nonce, &sealed[1 + NONCE_LEN..])
            .map_err(|_| SourceError::Seal("decryption failed: wrong key or tampered value".into()))
    }
}

/// Seal any serializable value as JSON.
pub fn seal_json<T: Serialize>(key: &SecretKey, value: &T) -> Result<Vec<u8>, SourceError> {
    let json =
        serde_json::to_vec(value).map_err(|e| SourceError::Payload(format!("serialize: {e}")))?;
    key.seal(&json)
}

/// Open a value sealed by [`seal_json`].
pub fn open_json<T: DeserializeOwned>(key: &SecretKey, sealed: &[u8]) -> Result<T, SourceError> {
    let json = key.open(sealed)?;
    serde_json::from_slice(&json).map_err(|e| SourceError::Payload(format!("deserialize: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SecretKey {
        SecretKey::from_bytes([7u8; 32])
    }

    #[test]
    fn seal_open_roundtrips() {
        let k = key();
        let sealed = k.seal(b"hunter2").expect("seal");
        assert_ne!(sealed.as_slice(), b"hunter2", "ciphertext is not plaintext");
        assert_eq!(k.open(&sealed).expect("open"), b"hunter2");
    }

    #[test]
    fn sealing_twice_gives_different_ciphertext() {
        let k = key();
        assert_ne!(
            k.seal(b"same").expect("a"),
            k.seal(b"same").expect("b"),
            "nonce must be random per write"
        );
    }

    #[test]
    fn wrong_key_fails_to_open() {
        let sealed = key().seal(b"secret").expect("seal");
        let other = SecretKey::from_bytes([9u8; 32]);
        assert!(other.open(&sealed).is_err());
    }

    #[test]
    fn truncated_ciphertext_fails_cleanly() {
        let sealed = key().seal(b"secret").expect("seal");
        assert!(key().open(&sealed[..4]).is_err());
        assert!(key().open(&[]).is_err());
    }

    #[test]
    fn version_byte_is_rejected_when_unknown() {
        let mut sealed = key().seal(b"secret").expect("seal");
        sealed[0] = 0xFF;
        assert!(key().open(&sealed).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails_to_open() {
        let mut sealed = key().seal(b"secret").expect("seal");
        let last = sealed.len() - 1;
        sealed[last] ^= 0x01;
        assert!(key().open(&sealed).is_err(), "AEAD must reject tampering");
    }

    #[test]
    fn json_roundtrips() {
        let k = key();
        let sealed = seal_json(&k, &vec!["a".to_string(), "b".to_string()]).expect("seal");
        let back: Vec<String> = open_json(&k, &sealed).expect("open");
        assert_eq!(back, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn debug_never_prints_key_material() {
        let rendered = format!("{:?}", SecretKey::from_bytes([0xAB; 32]));
        assert_eq!(rendered, "SecretKey(<redacted>)");
        assert!(!rendered.contains("ab"), "key bytes must not appear");
    }
}
