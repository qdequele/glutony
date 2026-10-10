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

/// Every plugin that pays a provider (calls [`ProviderCosts::apply`]), by plugin name.
/// The worker checks at boot that each registered one has a price entry. A plugin that
/// starts calling `apply` must be added here (the worker tests this against the
/// plugins' `NAME`s).
pub const PRICED_PLUGINS: &[&str] = &[
    "llm_enricher",
    "jev_enricher",
    "image_captioner",
    "whisper_transcriber",
];

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

    /// Load the table: the bundled one for `None`, else the file at `path` (an unreadable
    /// or invalid file is an `Err`, never a silent fallback).
    pub fn load(path: Option<&str>) -> Result<Self, String> {
        let Some(path) = path else {
            return Ok(Self::bundled());
        };
        let source = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read provider cost file {path}: {e}"))?;
        Self::from_toml(&source).map_err(|e| format!("{path}: {e}"))
    }

    /// The table to use when loading failed: EMPTY, so every call is flagged unpriced
    /// (`cost_complete: false`) instead of being billed at the bundled placeholder rates.
    pub fn or_unpriced(loaded: Result<Self, String>) -> Self {
        loaded.unwrap_or_default()
    }

    /// `PROVIDER_COSTS_FILE`, when set and non-blank.
    pub fn env_path() -> Option<String> {
        std::env::var("PROVIDER_COSTS_FILE")
            .ok()
            .filter(|p| !p.trim().is_empty())
    }

    /// Load the table `PROVIDER_COSTS_FILE` selects. The worker calls this at boot so a
    /// bad file stops it before it takes traffic.
    pub fn load_from_env() -> Result<Self, String> {
        Self::load(Self::env_path().as_deref())
    }

    /// The process-wide table: `PROVIDER_COSTS_FILE` if set, else the bundled one. If the
    /// file turns out unusable after boot, this logs an error and prices nothing (every
    /// call is flagged unpriced); it never falls back to the bundled placeholder rates.
    pub fn global() -> &'static ProviderCosts {
        static GLOBAL: OnceLock<ProviderCosts> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            let loaded = Self::load_from_env();
            if let Err(e) = &loaded {
                tracing::error!(error = %e, "cannot load PROVIDER_COSTS_FILE; every call will be flagged unpriced");
            }
            Self::or_unpriced(loaded)
        })
    }

    /// The price of `model` for `plugin`, falling back to the plugin's `default`.
    pub fn price(&self, plugin: &str, model: &str) -> Option<Price> {
        let models = self.entries.get(plugin)?;
        models.get(model).or_else(|| models.get("default")).copied()
    }

    /// The `registered` plugins that pay a provider ([`PRICED_PLUGINS`]) but have no
    /// entry at all in this table (neither a model nor `default`): every call they make
    /// is billed at 0 and flagged unpriced. In `registered` order.
    pub fn unpriced_plugins<'a>(&self, registered: &[&'a str]) -> Vec<&'a str> {
        registered
            .iter()
            .copied()
            .filter(|p| PRICED_PLUGINS.contains(p))
            .filter(|p| self.entries.get(*p).is_none_or(HashMap::is_empty))
            .collect()
    }

    /// Price one provider call in place: add its cost to `units.cost_micro_usd`, or
    /// count it in `units.unpriced_calls` when the table cannot price it.
    pub fn apply(&self, plugin: &str, model: &str, units: &mut UsageUnits) {
        let Some(price) = self.price(plugin, model) else {
            warn_unpriced_once(plugin, model, UnpricedReason::UnknownModel);
            units.unpriced_calls = units.unpriced_calls.saturating_add(1);
            return;
        };
        let prices_tokens = price.input_per_mtok > 0 || price.output_per_mtok > 0;
        let no_tokens = units.llm_input_tokens == 0 && units.llm_output_tokens == 0;
        // NaN and infinity are as unusable as a missing duration.
        let no_seconds = !(units.audio_seconds.is_finite() && units.audio_seconds > 0.0);
        if (prices_tokens && no_tokens) || (price.per_audio_second > 0 && no_seconds) {
            // The table knows the model but the provider did not report the quantity.
            warn_unpriced_once(plugin, model, UnpricedReason::MissingQuantity);
            units.unpriced_calls = units.unpriced_calls.saturating_add(1);
            return;
        }
        // Saturating throughout: an absurd quantity must over-charge, never panic (debug)
        // or wrap into an under-charge (release).
        let tokens = u128::from(units.llm_input_tokens)
            .saturating_mul(u128::from(price.input_per_mtok))
            .saturating_add(
                u128::from(units.llm_output_tokens)
                    .saturating_mul(u128::from(price.output_per_mtok)),
            );
        let token_cost = tokens.div_ceil(1_000_000);
        // A float-to-int `as` cast saturates at the target's bounds and maps NaN to 0.
        let audio_cost = (units.audio_seconds * price.per_audio_second as f64).ceil() as u128;
        let requests = units
            .llm_requests
            .saturating_add(units.external_requests)
            .max(1);
        let request_cost = u128::from(requests).saturating_mul(u128::from(price.per_request));
        let total = token_cost
            .saturating_add(audio_cost)
            .saturating_add(request_cost);
        units.cost_micro_usd = units
            .cost_micro_usd
            .saturating_add(u64::try_from(total).unwrap_or(u64::MAX));
    }
}

/// Why a call could not be priced; each reason warns once per `(plugin, model)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum UnpricedReason {
    UnknownModel,
    MissingQuantity,
}

/// Log once per `(plugin, model, reason)`. Returns whether this call was the first.
fn warn_unpriced_once(plugin: &str, model: &str, reason: UnpricedReason) -> bool {
    static SEEN: OnceLock<Mutex<HashSet<(String, String, UnpricedReason)>>> = OnceLock::new();
    let seen = SEEN.get_or_init(|| Mutex::new(HashSet::new()));
    let Ok(mut seen) = seen.lock() else {
        return false;
    };
    if !seen.insert((plugin.to_string(), model.to_string(), reason)) {
        return false;
    }
    match reason {
        UnpricedReason::UnknownModel => tracing::warn!(
            plugin,
            model,
            "no provider price for this model; its calls are billed at 0 and flagged"
        ),
        UnpricedReason::MissingQuantity => tracing::warn!(
            plugin,
            model,
            "the provider did not report token counts or audio duration; these calls are billed at 0 and flagged"
        ),
    }
    true
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
    fn a_missing_quantity_warns_once_per_reason_and_still_counts() {
        // Unique names: the dedupe set is process-wide.
        let reason = UnpricedReason::MissingQuantity;
        assert!(warn_unpriced_once("p_warn", "m_warn", reason));
        assert!(!warn_unpriced_once("p_warn", "m_warn", reason));
        assert!(
            warn_unpriced_once("p_warn", "m_warn", UnpricedReason::UnknownModel),
            "a different reason is not suppressed"
        );
        let mut u = UsageUnits {
            llm_requests: 2,
            ..UsageUnits::default()
        };
        costs().apply("llm_enricher", "gpt-4o-mini", &mut u);
        assert_eq!((u.cost_micro_usd, u.unpriced_calls), (0, 1));
    }

    #[test]
    fn absurd_quantities_saturate_instead_of_panicking_or_wrapping() {
        let mut huge = UsageUnits::transcription(1e300);
        costs().apply("whisper_transcriber", "whisper-1", &mut huge);
        assert_eq!(huge.cost_micro_usd, u64::MAX);

        let mut nan = UsageUnits {
            audio_seconds: f64::NAN,
            external_requests: 1,
            ..UsageUnits::default()
        };
        costs().apply("whisper_transcriber", "whisper-1", &mut nan);
        assert_eq!((nan.cost_micro_usd, nan.unpriced_calls), (0, 1));

        let mut near_max = UsageUnits {
            cost_micro_usd: u64::MAX - 1,
            ..UsageUnits::llm(1_000, 100)
        };
        costs().apply("llm_enricher", "gpt-4o-mini", &mut near_max);
        assert_eq!(near_max.cost_micro_usd, u64::MAX);

        let mut many = UsageUnits {
            llm_requests: u64::MAX,
            external_requests: u64::MAX,
            ..UsageUnits::llm(u64::MAX, u64::MAX)
        };
        costs().apply("jev_enricher", "jev-latest", &mut many);
        assert_eq!(many.cost_micro_usd, u64::MAX);
    }

    #[test]
    fn a_bad_table_is_an_error() {
        assert!(ProviderCosts::from_toml("[llm_enricher.m]\ninput_per_mtok = -1").is_err());
        assert!(ProviderCosts::from_toml("[llm_enricher.m]\ntypo = 1").is_err());
    }

    #[test]
    fn load_none_is_the_bundled_table() {
        assert_eq!(ProviderCosts::load(None).unwrap(), ProviderCosts::bundled());
    }

    #[test]
    fn load_reads_a_file_and_rejects_a_missing_or_invalid_one() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("costs.toml");
        std::fs::write(&good, TABLE).unwrap();
        let loaded = ProviderCosts::load(Some(good.to_str().unwrap())).unwrap();
        assert_eq!(loaded, costs());
        assert_ne!(loaded, ProviderCosts::bundled());

        let missing = dir.path().join("nope.toml");
        let err = ProviderCosts::load(Some(missing.to_str().unwrap())).unwrap_err();
        assert!(err.contains("nope.toml"), "{err}");

        let bad = dir.path().join("bad.toml");
        std::fs::write(&bad, "[llm_enricher.m]\ntypo = 1").unwrap();
        let err = ProviderCosts::load(Some(bad.to_str().unwrap())).unwrap_err();
        assert!(err.contains("invalid provider cost table"), "{err}");
    }

    #[test]
    fn a_failed_load_prices_nothing_instead_of_using_the_bundled_rates() {
        let table = ProviderCosts::or_unpriced(Err("boom".into()));
        assert_eq!(table, ProviderCosts::default());
        let mut u = UsageUnits::llm(1_000, 100);
        table.apply("llm_enricher", "gpt-4o-mini", &mut u);
        assert_eq!((u.cost_micro_usd, u.unpriced_calls), (0, 1));
        // A successful load is passed through untouched.
        assert_eq!(ProviderCosts::or_unpriced(Ok(costs())), costs());
    }

    #[test]
    fn plugins_without_any_price_entry_are_listed() {
        let table = ProviderCosts::from_toml(
            r#"
            [llm_enricher."gpt-4o-mini"]
            input_per_mtok = 1
            [jev_enricher.default]
            per_request = 1
            "#,
        )
        .unwrap();
        let registered = [
            "pdf_extractor",
            "llm_enricher",
            "image_captioner",
            "jev_enricher",
            "whisper_transcriber",
        ];
        assert_eq!(
            table.unpriced_plugins(&registered),
            ["image_captioner", "whisper_transcriber"],
            "provider plugins only: pdf_extractor pays no provider"
        );
        // A plugin that is not registered is not reported.
        assert_eq!(
            table.unpriced_plugins(&["llm_enricher"]),
            Vec::<&str>::new()
        );
        // An empty table prices none of them.
        assert_eq!(
            ProviderCosts::default().unpriced_plugins(PRICED_PLUGINS),
            PRICED_PLUGINS
        );
    }

    #[test]
    fn the_bundled_table_leaves_only_jev_enricher_unpriced() {
        assert_eq!(
            ProviderCosts::bundled().unpriced_plugins(PRICED_PLUGINS),
            ["jev_enricher"]
        );
    }

    #[test]
    fn the_k8s_table_is_the_bundled_one_and_the_jev_template_parses() {
        let k8s = include_str!("../../../k8s/provider-costs.toml");
        let (_note, body) = k8s.split_once('\n').unwrap();
        assert_eq!(
            body, BUNDLED,
            "k8s/provider-costs.toml drifted from config/"
        );
        assert!(BUNDLED.contains("# NOT PRODUCTION PRICES"));
        let template: String = BUNDLED
            .lines()
            .skip_while(|l| *l != "# [jev_enricher.default]")
            .map(|l| format!("{}\n", l.trim_start_matches("# ")))
            .collect();
        let table = ProviderCosts::from_toml(&template).unwrap();
        assert!(table.price("jev_enricher", "jev-latest").is_some());
    }

    #[test]
    fn the_bundled_table_parses() {
        let b = ProviderCosts::bundled();
        assert!(b.price("llm_enricher", "gpt-4o-mini").is_some());
        assert!(b.price("whisper_transcriber", "whisper-1").is_some());
    }
}
