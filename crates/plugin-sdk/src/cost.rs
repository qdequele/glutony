//! Provider cost table: what glutony pays a provider for one call (spec §5.1).
//!
//! Prices are micro-USD, keyed by `(plugin, model)` with an optional `default` entry
//! per plugin. The bundled table (`config/provider-costs.toml`) is compiled in;
//! `PROVIDER_COSTS_FILE` replaces it. A call the table cannot price costs 0 and counts
//! in [`UsageUnits::unpriced_calls`], so the bill shows the gap instead of hiding it.
//! The provider behind a model depends on the deployment's `*_BASE_URL`: operators
//! who point a plugin at another provider must ship their own table.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use serde::Deserialize;

use crate::types::UsageUnits;

/// The bundled table.
const BUNDLED: &str = include_str!("../../../config/provider-costs.toml");

/// Price of one model, in micro-USD. Absent dimensions cost nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    /// Per million prompt tokens.
    #[serde(default)]
    pub input_per_mtok: u64,
    /// Per million completion tokens.
    #[serde(default)]
    pub output_per_mtok: u64,
    /// Per second of audio.
    #[serde(default)]
    pub per_audio_second: u64,
    /// Per request.
    #[serde(default)]
    pub per_request: u64,
}

/// The whole table: plugin name → model (or `default`) → price.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProviderCosts {
    entries: HashMap<String, HashMap<String, Price>>,
}

impl ProviderCosts {
    /// Parse a TOML table.
    pub fn from_toml(source: &str) -> Result<Self, String> {
        let entries: HashMap<String, HashMap<String, Price>> =
            toml::from_str(source).map_err(|e| format!("invalid provider cost table: {e}"))?;
        Ok(Self { entries })
    }

    /// The compiled-in table.
    pub fn bundled() -> Self {
        // The bundled file is tested (`the_bundled_table_parses`); an empty table is
        // the safe fallback that flags every call as unpriced.
        Self::from_toml(BUNDLED).unwrap_or_default()
    }

    /// The process-wide table: `PROVIDER_COSTS_FILE` if set, else the bundled one.
    /// An unreadable or invalid file logs an error and falls back to the bundled table.
    pub fn global() -> &'static ProviderCosts {
        static GLOBAL: OnceLock<ProviderCosts> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            let Some(path) = std::env::var("PROVIDER_COSTS_FILE")
                .ok()
                .filter(|p| !p.trim().is_empty())
            else {
                return Self::bundled();
            };
            match std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|s| Self::from_toml(&s))
            {
                Ok(costs) => costs,
                Err(e) => {
                    tracing::error!(path, error = %e, "cannot load PROVIDER_COSTS_FILE; using the bundled table");
                    Self::bundled()
                }
            }
        })
    }

    /// The price of `model` for `plugin`, falling back to the plugin's `default`.
    pub fn price(&self, plugin: &str, model: &str) -> Option<Price> {
        let models = self.entries.get(plugin)?;
        models.get(model).or_else(|| models.get("default")).copied()
    }

    /// Price one provider call in place: add its cost to `units.cost_micro_usd`, or
    /// count it in `units.unpriced_calls` when the table cannot price it.
    pub fn apply(&self, plugin: &str, model: &str, units: &mut UsageUnits) {
        let Some(price) = self.price(plugin, model) else {
            warn_unpriced_once(plugin, model);
            units.unpriced_calls += 1;
            return;
        };
        let prices_tokens = price.input_per_mtok > 0 || price.output_per_mtok > 0;
        let no_tokens = units.llm_input_tokens == 0 && units.llm_output_tokens == 0;
        let no_seconds = units.audio_seconds <= 0.0;
        if (prices_tokens && no_tokens) || (price.per_audio_second > 0 && no_seconds) {
            // The table knows the model but the provider did not report the quantity.
            units.unpriced_calls += 1;
            return;
        }
        let tokens = u128::from(units.llm_input_tokens) * u128::from(price.input_per_mtok)
            + u128::from(units.llm_output_tokens) * u128::from(price.output_per_mtok);
        let token_cost = tokens.div_ceil(1_000_000);
        let audio_cost = (units.audio_seconds * price.per_audio_second as f64).ceil() as u128;
        let requests = u128::from(units.llm_requests + units.external_requests).max(1);
        let request_cost = requests * u128::from(price.per_request);
        let total = token_cost + audio_cost + request_cost;
        units.cost_micro_usd += u64::try_from(total).unwrap_or(u64::MAX);
    }
}

/// Log once per `(plugin, model)` that it has no price.
fn warn_unpriced_once(plugin: &str, model: &str) {
    static SEEN: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let seen = SEEN.get_or_init(|| Mutex::new(HashSet::new()));
    if let Ok(mut seen) = seen.lock()
        && seen.insert((plugin.to_string(), model.to_string()))
    {
        tracing::warn!(
            plugin,
            model,
            "no provider price for this model; its calls are billed at 0 and flagged"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = r#"
        [llm_enricher."gpt-4o-mini"]
        input_per_mtok = 150000
        output_per_mtok = 600000

        [whisper_transcriber."whisper-1"]
        per_audio_second = 100

        [jev_enricher.default]
        per_request = 250
    "#;

    fn costs() -> ProviderCosts {
        ProviderCosts::from_toml(TABLE).unwrap()
    }

    #[test]
    fn prices_tokens_rounding_up_to_the_micro_dollar() {
        let mut u = UsageUnits::llm(1_000, 100);
        costs().apply("llm_enricher", "gpt-4o-mini", &mut u);
        // 1000 * 0.15 + 100 * 0.6 = 150 + 60 micro-USD
        assert_eq!(u.cost_micro_usd, 210);
        assert_eq!(u.unpriced_calls, 0);
        let mut tiny = UsageUnits::llm(1, 0);
        costs().apply("llm_enricher", "gpt-4o-mini", &mut tiny);
        assert_eq!(tiny.cost_micro_usd, 1, "0.15 micro-USD rounds up");
    }

    #[test]
    fn prices_audio_seconds_and_requests() {
        let mut u = UsageUnits::transcription(12.5);
        costs().apply("whisper_transcriber", "whisper-1", &mut u);
        assert_eq!(u.cost_micro_usd, 1_250);
        let mut j = UsageUnits::llm(10, 2);
        costs().apply("jev_enricher", "jev-latest", &mut j);
        assert_eq!(j.cost_micro_usd, 250, "the plugin's default entry applies");
    }

    #[test]
    fn unknown_models_and_unknown_quantities_are_counted_not_guessed() {
        let mut u = UsageUnits::llm(1_000, 100);
        costs().apply("llm_enricher", "gpt-9", &mut u);
        assert_eq!((u.cost_micro_usd, u.unpriced_calls), (0, 1));
        // The provider returned no token counts.
        let mut no_tokens = UsageUnits {
            llm_requests: 1,
            ..UsageUnits::default()
        };
        costs().apply("llm_enricher", "gpt-4o-mini", &mut no_tokens);
        assert_eq!((no_tokens.cost_micro_usd, no_tokens.unpriced_calls), (0, 1));
        // The transcription duration is unknown.
        let mut no_seconds = UsageUnits {
            external_requests: 1,
            ..UsageUnits::default()
        };
        costs().apply("whisper_transcriber", "whisper-1", &mut no_seconds);
        assert_eq!(
            (no_seconds.cost_micro_usd, no_seconds.unpriced_calls),
            (0, 1)
        );
    }

    #[test]
    fn a_bad_table_is_an_error() {
        assert!(ProviderCosts::from_toml("[llm_enricher.m]\ninput_per_mtok = -1").is_err());
        assert!(ProviderCosts::from_toml("[llm_enricher.m]\ntypo = 1").is_err());
    }

    #[test]
    fn the_bundled_table_parses() {
        let b = ProviderCosts::bundled();
        assert!(b.price("llm_enricher", "gpt-4o-mini").is_some());
        assert!(b.price("whisper_transcriber", "whisper-1").is_some());
    }
}
