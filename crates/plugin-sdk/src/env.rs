//! Boolean environment flags, parsed the same way by the gateway, the control plane
//! and the workers.
//!
//! `true`/`1`/`yes`/`on` turn a flag on and `false`/`0`/`no`/`off` turn it off, in any
//! case and with surrounding whitespace. Unset or blank is off (Compose passes
//! `${VAR:-}`, an empty string). Any other value is off too, with a warning naming the
//! variable: a typo never stops a binary from booting, and it never turns a flag on.

/// The value of the boolean flag `name`, from its raw value (`None` = unset). See the
/// [module docs](self) for what is accepted.
pub fn flag(name: &str, raw: Option<&str>) -> bool {
    let value = raw.map(str::trim).unwrap_or("");
    match recognise(value) {
        Some(on) => on,
        None => {
            tracing::warn!(
                variable = name,
                value,
                "{name} is not a boolean (true/false, 1/0, yes/no, on/off); treating it as false"
            );
            false
        }
    }
}

/// [`flag`] for the process environment variable `name`.
pub fn env_flag(name: &str) -> bool {
    flag(name, std::env::var(name).ok().as_deref())
}

/// `Some(on)` for a recognised (trimmed) value, `None` for anything else.
fn recognise(value: &str) -> Option<bool> {
    const ON: [&str; 4] = ["true", "1", "yes", "on"];
    const OFF: [&str; 5] = ["", "false", "0", "no", "off"];
    if ON.iter().any(|v| value.eq_ignore_ascii_case(v)) {
        Some(true)
    } else if OFF.iter().any(|v| value.eq_ignore_ascii_case(v)) {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_spelling_of_on_and_off() {
        for raw in ["true", "TRUE", " True\n", "1", "yes", "YES", "on", "On"] {
            assert!(flag("X", Some(raw)), "{raw:?}");
        }
        for raw in ["false", "FALSE", "0", "no", "No", "off", "OFF", "", "  "] {
            assert!(!flag("X", Some(raw)), "{raw:?}");
        }
        assert!(!flag("X", None), "unset is off");
    }

    #[test]
    fn an_unrecognised_value_is_off() {
        for raw in ["enabled", "y", "2", "tru", "true!"] {
            assert_eq!(recognise(raw), None, "{raw:?}");
            assert!(!flag("X", Some(raw)), "{raw:?}");
        }
    }
}
