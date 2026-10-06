//! Tenant ids: the opaque owner of pipelines, sources, connections and jobs.
//!
//! Glutony never interprets a tenant id. Behind the Meilisearch Lab it is an account
//! UUID; behind Meilisearch Cloud's Envoy it is a project id; standalone it is absent.
//! It is validated wherever it enters so it is always safe to log and to put in a
//! query string.

/// Longest accepted tenant id.
pub const MAX_TENANT_ID_LEN: usize = 128;

/// Check a tenant id: 1 to [`MAX_TENANT_ID_LEN`] characters of `[A-Za-z0-9._:-]`.
pub fn validate_tenant_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > MAX_TENANT_ID_LEN {
        return Err(format!(
            "tenant id must be 1 to {MAX_TENANT_ID_LEN} characters"
        ));
    }
    if let Some(c) = id
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-')))
    {
        return Err(format!(
            "tenant id contains {c:?}; allowed characters are A-Z a-z 0-9 . _ : -"
        ));
    }
    Ok(())
}

/// Whose a pipeline, source or connection is, as seen by the caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowScope {
    /// Owned by the caller's tenant.
    Tenant,
    /// Shared by every tenant; read-only for tenants.
    #[default]
    Global,
    /// Compiled into glutony; read-only for everyone.
    Builtin,
}

impl RowScope {
    /// The scope of a row from its flags.
    pub fn of(builtin: bool, tenant_id: Option<&str>) -> Self {
        match (builtin, tenant_id) {
            (true, _) => RowScope::Builtin,
            (false, Some(_)) => RowScope::Tenant,
            (false, None) => RowScope::Global,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_uuids_project_ids_and_the_allowed_punctuation() {
        for ok in [
            "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61",
            "hackersearch",
            "acme.eu:prod_1",
            &"a".repeat(MAX_TENANT_ID_LEN),
        ] {
            assert_eq!(validate_tenant_id(ok), Ok(()), "{ok:?}");
        }
    }

    #[test]
    fn rejects_empty_too_long_and_other_characters() {
        for bad in [
            String::new(),
            "a".repeat(MAX_TENANT_ID_LEN + 1),
            "a/b".into(),
            "a b".into(),
            "a%2Fb".into(),
            "é".into(),
            "a\nb".into(),
        ] {
            assert!(
                validate_tenant_id(&bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn row_scope_of_a_row() {
        assert_eq!(RowScope::of(true, None), RowScope::Builtin);
        assert_eq!(RowScope::of(false, None), RowScope::Global);
        assert_eq!(RowScope::of(false, Some("t1")), RowScope::Tenant);
        assert_eq!(serde_json::to_value(RowScope::Tenant).unwrap(), "tenant");
    }
}
