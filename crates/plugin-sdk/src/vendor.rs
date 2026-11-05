//! Model ids on a direct, self-hosted call to a vendor.
//!
//! The Lab's Gateway (Lumen) names every model `provider/model` (`openai/gpt-4o-mini`,
//! `typesafe/jev`), and pipeline examples use that form so they run unchanged behind the
//! Lab. Self-hosted, an AI plugin calls the vendor directly, and the vendor only knows its
//! own bare ids. [`direct_model`] strips the Gateway prefix when it names the vendor the
//! call goes to, and leaves every other id alone.

/// Public API hosts of the vendors an AI plugin can call directly, with the Gateway
/// provider slug that names each one. A base URL on any other host (a proxy, a
/// self-hosted Gateway, vLLM, Ollama…) keeps the model id as written: there, a `/` in
/// the id is either the Gateway's prefix or part of the model's own name
/// (`meta-llama/Llama-3.1-8B`).
const VENDOR_HOSTS: &[(&str, &str)] = &[
    ("api.openai.com", "openai"),
    ("api.typesafe.ai", "typesafe"),
    ("api.anthropic.com", "anthropic"),
    ("api.mistral.ai", "mistral"),
    ("api.groq.com", "groq"),
    ("api.together.xyz", "together"),
    ("api.perplexity.ai", "perplexity"),
];

/// The Gateway provider slug of the vendor whose public API `base_url` points at, if known.
pub fn vendor_for_base_url(base_url: &str) -> Option<&'static str> {
    let host = host_of(base_url)?;
    VENDOR_HOSTS
        .iter()
        .find(|(h, _)| host.eq_ignore_ascii_case(h))
        .map(|(_, slug)| *slug)
}

/// The model id to send when calling `base_url` directly: `model` without a leading
/// `<provider>/` when that provider is the vendor behind `base_url`
/// (`openai/gpt-4o-mini` → `gpt-4o-mini` on `https://api.openai.com/v1`), else `model`
/// unchanged. Bare ids, another provider's prefix and unknown hosts are left as they are.
///
/// Only for direct calls: a Gateway (the Lab's target) expects the prefixed id.
pub fn direct_model<'a>(base_url: &str, model: &'a str) -> &'a str {
    let Some(vendor) = vendor_for_base_url(base_url) else {
        return model;
    };
    match model.split_once('/') {
        Some((provider, bare)) if provider.eq_ignore_ascii_case(vendor) && !bare.is_empty() => bare,
        _ => model,
    }
}

/// The host of `url`, without scheme, credentials, port or path.
fn host_of(url: &str) -> Option<&str> {
    let rest = url.trim().split_once("://").map_or(url.trim(), |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = authority.split(':').next()?;
    (!host.is_empty()).then_some(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_vendors_own_prefix_is_stripped() {
        assert_eq!(
            direct_model("https://api.openai.com/v1", "openai/gpt-4o-mini"),
            "gpt-4o-mini"
        );
        assert_eq!(
            direct_model("https://api.typesafe.ai/v1", "typesafe/jev"),
            "jev"
        );
        assert_eq!(
            direct_model("https://api.groq.com/openai/v1", "groq/whisper-large-v3"),
            "whisper-large-v3"
        );
        assert_eq!(
            direct_model("HTTPS://API.OpenAI.com:443/v1/", "OpenAI/gpt-4o"),
            "gpt-4o"
        );
    }

    #[test]
    fn bare_ids_are_sent_unchanged() {
        assert_eq!(
            direct_model("https://api.openai.com/v1", "gpt-4o-mini"),
            "gpt-4o-mini"
        );
        assert_eq!(
            direct_model("https://api.typesafe.ai/v1", "jev-latest"),
            "jev-latest"
        );
    }

    #[test]
    fn another_providers_prefix_is_kept() {
        assert_eq!(
            direct_model("https://api.openai.com/v1", "typesafe/jev"),
            "typesafe/jev"
        );
        // Groq serves OpenAI's open-weight models under their own `openai/` id.
        assert_eq!(
            direct_model("https://api.groq.com/openai/v1", "openai/gpt-oss-20b"),
            "openai/gpt-oss-20b"
        );
    }

    #[test]
    fn unknown_hosts_keep_the_id_as_written() {
        for base in [
            "https://gateway.example.com/v1",
            "http://localhost:11434/v1",
            "http://vllm:8000/v1",
            "not a url",
            "",
        ] {
            assert_eq!(
                direct_model(base, "openai/gpt-4o-mini"),
                "openai/gpt-4o-mini"
            );
            assert_eq!(
                direct_model(base, "meta-llama/Llama-3.1-8B"),
                "meta-llama/Llama-3.1-8B"
            );
        }
    }

    #[test]
    fn a_prefix_with_nothing_after_it_is_kept() {
        assert_eq!(
            direct_model("https://api.openai.com/v1", "openai/"),
            "openai/"
        );
    }

    #[test]
    fn hosts_are_parsed_without_credentials_port_or_path() {
        assert_eq!(
            host_of("https://u:p@api.openai.com:8443/v1?x=1"),
            Some("api.openai.com")
        );
        assert_eq!(host_of("api.openai.com/v1"), Some("api.openai.com"));
        assert_eq!(
            vendor_for_base_url("https://api.openai.com.evil.example/v1"),
            None
        );
    }
}
