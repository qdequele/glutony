//! Operator-level bounds on what one record-format step may decode.
//!
//! `parquet_parser`, `avro_parser` and `msgpack_parser` turn compact binary input
//! into JSON values held in the worker's memory, which every activity on the task
//! queue shares. A few-KB Parquet file with one run-length encoded column can
//! hold a billion rows, so the upload size bounds nothing. [`DecodeLimits`] caps
//! the rows a step may produce and the memory its decoded values may take. The
//! worker sets them from `PARSER_MAX_ROWS` and `PARSER_MAX_DECODED_MB`; a pipeline
//! cannot raise them (its `max_rows` / `max_records` only lower the row count, and
//! keep their meaning: truncate). Going over a limit fails the step as
//! [`PluginError::InvalidInput`]: nothing is truncated, so nothing is silently lost.

use meili_ingest_plugin_sdk::PluginError;
use serde_json::Value;

/// Default of `PARSER_MAX_ROWS`.
pub const DEFAULT_MAX_ROWS: u64 = 1_000_000;

/// Default of `PARSER_MAX_DECODED_MB`, in bytes (512 MiB). The documents built
/// from the values and the step output that carries them roughly double that at
/// peak, which keeps one step well inside a 2 GiB worker.
pub const DEFAULT_MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;

/// Bounds on one decode. See the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Most rows (records, documents) one step may produce (`PARSER_MAX_ROWS`).
    pub max_rows: u64,
    /// Most memory the decoded values of one step may take, as estimated by
    /// [`decoded_size`] (`PARSER_MAX_DECODED_MB`).
    pub max_decoded_bytes: u64,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_decoded_bytes: DEFAULT_MAX_DECODED_BYTES,
        }
    }
}

/// Memory one decoded value node takes before its heap data.
pub const NODE_BYTES: u64 = std::mem::size_of::<Value>() as u64;

/// Estimated memory a decoded [`Value`] takes: one [`NODE_BYTES`] per node, plus
/// the text of strings and object keys. It is what [`DecodeLimits::max_decoded_bytes`]
/// is measured in.
pub fn decoded_size(value: &Value) -> u64 {
    match value {
        Value::String(s) => NODE_BYTES + s.len() as u64,
        Value::Array(items) => NODE_BYTES + items.iter().map(decoded_size).sum::<u64>(),
        Value::Object(map) => {
            NODE_BYTES
                + map
                    .iter()
                    .map(|(k, v)| {
                        std::mem::size_of::<String>() as u64 + k.len() as u64 + decoded_size(v)
                    })
                    .sum::<u64>()
        }
        _ => NODE_BYTES,
    }
}

/// The running tally of one decode against its [`DecodeLimits`].
#[derive(Debug)]
pub struct DecodeBudget {
    limits: DecodeLimits,
    label: &'static str,
    rows: u64,
    bytes: u64,
}

impl DecodeBudget {
    /// A fresh tally. `label` (the plugin name) prefixes the errors.
    pub fn new(limits: DecodeLimits, label: &'static str) -> Self {
        Self {
            limits,
            label,
            rows: 0,
            bytes: 0,
        }
    }

    /// Fail unless a payload of `rows` rows in total stays within the row limit.
    /// Does not count them: for an up-front check against a declared row count.
    pub fn check_rows(&self, rows: u64) -> Result<(), PluginError> {
        if rows > self.limits.max_rows {
            return Err(self.too_many_rows());
        }
        Ok(())
    }

    /// Count `n` more rows, failing past the row limit.
    pub fn add_rows(&mut self, n: u64) -> Result<(), PluginError> {
        self.rows = self.rows.saturating_add(n);
        if self.rows > self.limits.max_rows {
            return Err(self.too_many_rows());
        }
        Ok(())
    }

    /// Fail unless `n` more bytes would still fit. Does not count them: for a cheap
    /// lower bound checked before the expensive decode.
    pub fn check_bytes(&self, n: u64) -> Result<(), PluginError> {
        if self.bytes.saturating_add(n) > self.limits.max_decoded_bytes {
            return Err(self.too_large());
        }
        Ok(())
    }

    /// Count `n` more decoded bytes, failing past the size limit.
    pub fn add_bytes(&mut self, n: u64) -> Result<(), PluginError> {
        self.bytes = self.bytes.saturating_add(n);
        if self.bytes > self.limits.max_decoded_bytes {
            return Err(self.too_large());
        }
        Ok(())
    }

    /// Rows counted so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    fn too_many_rows(&self) -> PluginError {
        PluginError::InvalidInput(format!(
            "{}: the payload holds more than {} rows, the most this worker decodes in one \
             step (PARSER_MAX_ROWS); nothing was indexed. Split the file, or ask the \
             operator to raise the limit",
            self.label, self.limits.max_rows
        ))
    }

    fn too_large(&self) -> PluginError {
        PluginError::InvalidInput(format!(
            "{}: the payload takes more than {} MiB once decoded, the most this worker \
             decodes in one step (PARSER_MAX_DECODED_MB); nothing was indexed. Split the \
             file, or ask the operator to raise the limit",
            self.label,
            self.limits.max_decoded_bytes.div_ceil(1024 * 1024)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decoded_size_counts_nodes_and_text() {
        assert_eq!(decoded_size(&json!(null)), NODE_BYTES);
        assert_eq!(decoded_size(&json!("abc")), NODE_BYTES + 3);
        assert_eq!(decoded_size(&json!([1, 2])), 3 * NODE_BYTES);
        let string = std::mem::size_of::<String>() as u64;
        assert_eq!(
            decoded_size(&json!({"ab": "c"})),
            NODE_BYTES + string + 2 + NODE_BYTES + 1
        );
    }

    #[test]
    fn the_budget_fails_past_each_limit_and_not_at_it() {
        let limits = DecodeLimits {
            max_rows: 2,
            max_decoded_bytes: 10,
        };
        let mut b = DecodeBudget::new(limits, "x");
        b.add_rows(2).unwrap();
        assert!(
            matches!(b.add_rows(1), Err(PluginError::InvalidInput(m)) if m.contains("PARSER_MAX_ROWS"))
        );
        let mut b = DecodeBudget::new(limits, "x");
        b.check_rows(2).unwrap();
        assert!(b.check_rows(3).is_err());
        b.add_bytes(10).unwrap();
        assert!(b.check_bytes(1).is_err());
        assert!(
            matches!(b.add_bytes(1), Err(PluginError::InvalidInput(m)) if m.contains("PARSER_MAX_DECODED_MB"))
        );
    }
}
