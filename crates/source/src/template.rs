//! URL templating.
//!
//! A source's URL is rendered against the run's **scheduled** time, never wall-clock: a
//! retry must resolve the same URL, and a backfill of 2026-09-01 must fetch that day's
//! file rather than today's (Decision 9).
//!
//! | Token | Expands to |
//! |---|---|
//! | `{{ date:FMT }}` | `strftime(FMT)` of the scheduled time |
//! | `{{ date-1d:FMT }}` | Same, offset by a duration (`-1d`, `+2d`, `-3h`, `+30m`) |
//! | `{{ timestamp }}` | Unix seconds of the scheduled time |
//!
//! The offset form is what makes the TMDB export work: its daily file is published
//! around 08:00 UTC, so a schedule firing at 00:30 must ask for `{{ date-1d:… }}`.

use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;

use crate::SourceError;

/// Render every `{{ … }}` token in `template`.
///
/// Unknown tokens are an error rather than a silent passthrough: a typo in a date format
/// should fail when the source is created, not fetch a 404 every night forever.
pub fn render(
    template: &str,
    scheduled_at: DateTime<Utc>,
    timezone: &str,
) -> Result<String, SourceError> {
    let tz: Tz = timezone
        .parse()
        .map_err(|_| SourceError::Template(format!("unknown timezone {timezone:?}")))?;

    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find("}}")
            .ok_or_else(|| SourceError::Template("unterminated {{ token".into()))?;
        out.push_str(&expand(after[..end].trim(), scheduled_at, tz)?);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Expand one token's inner text (already trimmed of surrounding whitespace).
fn expand(token: &str, at: DateTime<Utc>, tz: Tz) -> Result<String, SourceError> {
    if token == "timestamp" {
        return Ok(at.timestamp().to_string());
    }
    let (head, fmt) = token
        .split_once(':')
        .ok_or_else(|| SourceError::Template(format!("unknown token {{{{ {token} }}}}")))?;
    let head = head.trim();
    let fmt = fmt.trim();
    let offset = match head.strip_prefix("date") {
        Some("") => Duration::zero(),
        Some(raw) => parse_offset(raw)?,
        None => {
            return Err(SourceError::Template(format!(
                "unknown token {{{{ {token} }}}}"
            )));
        }
    };
    let shifted = at + offset;
    Ok(shifted.with_timezone(&tz).format(fmt).to_string())
}

/// Parse an offset such as `-1d`, `+2d`, `-3h`, `+30m`.
fn parse_offset(raw: &str) -> Result<Duration, SourceError> {
    let bad = || SourceError::Template(format!("invalid offset {raw:?}; expected e.g. -1d, +3h"));
    if raw.len() < 3 {
        return Err(bad());
    }
    let (sign, rest) = match raw.split_at(1) {
        ("-", rest) => (-1, rest),
        ("+", rest) => (1, rest),
        _ => return Err(bad()),
    };
    let (digits, unit) = rest.split_at(rest.len() - 1);
    let n: i64 = digits.parse().map_err(|_| bad())?;
    let magnitude = match unit {
        "d" => Duration::try_days(n),
        "h" => Duration::try_hours(n),
        "m" => Duration::try_minutes(n),
        _ => return Err(bad()),
    }
    .ok_or_else(bad)?;
    Ok(magnitude * sign)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0)
            .single()
            .expect("valid instant")
    }

    #[test]
    fn renders_a_date_token() {
        let out = render(
            "https://files.tmdb.org/p/exports/movie_ids_{{ date:%m_%d_%Y }}.json.gz",
            at(2026, 9, 13, 9),
            "UTC",
        )
        .expect("renders");
        assert_eq!(
            out,
            "https://files.tmdb.org/p/exports/movie_ids_09_13_2026.json.gz"
        );
    }

    #[test]
    fn renders_a_negative_day_offset() {
        // A schedule firing at 00:30 must fetch the PREVIOUS day's export, because the
        // current day's file does not exist until ~08:00 UTC.
        let out =
            render("x/{{ date-1d:%Y-%m-%d }}.json", at(2026, 9, 13, 0), "UTC").expect("renders");
        assert_eq!(out, "x/2026-09-12.json");
    }

    #[test]
    fn renders_positive_and_hour_offsets() {
        assert_eq!(
            render("{{ date+2d:%Y-%m-%d }}", at(2026, 9, 13, 0), "UTC").expect("renders"),
            "2026-09-15"
        );
        assert_eq!(
            render("{{ date-3h:%Y-%m-%dT%H }}", at(2026, 9, 13, 2), "UTC").expect("renders"),
            "2026-09-12T23"
        );
    }

    #[test]
    fn renders_timestamp() {
        let out = render("{{ timestamp }}", at(2026, 9, 13, 0), "UTC").expect("renders");
        assert_eq!(out, at(2026, 9, 13, 0).timestamp().to_string());
    }

    #[test]
    fn renders_in_the_sources_timezone() {
        // 2026-09-13T01:00Z is still 2026-09-12 in Los Angeles.
        let out = render(
            "{{ date:%Y-%m-%d }}",
            at(2026, 9, 13, 1),
            "America/Los_Angeles",
        )
        .expect("renders");
        assert_eq!(out, "2026-09-12");
    }

    #[test]
    fn multiple_tokens_and_whitespace_variants() {
        let out = render(
            "{{date:%Y}}/{{ date:%m }}/{{  date:%d  }}",
            at(2026, 9, 13, 0),
            "UTC",
        )
        .expect("renders");
        assert_eq!(out, "2026/09/13");
    }

    #[test]
    fn a_template_without_tokens_is_returned_unchanged() {
        let out =
            render("https://example.test/feed.json", at(2026, 9, 13, 0), "UTC").expect("renders");
        assert_eq!(out, "https://example.test/feed.json");
    }

    #[test]
    fn unknown_tokens_are_rejected_not_passed_through() {
        assert!(render("{{ nope }}", at(2026, 9, 13, 0), "UTC").is_err());
        assert!(render("{{ date }}", at(2026, 9, 13, 0), "UTC").is_err());
        assert!(render("{{ date-1x:%Y }}", at(2026, 9, 13, 0), "UTC").is_err());
        assert!(render("{{ date-:%Y }}", at(2026, 9, 13, 0), "UTC").is_err());
        assert!(render("{{ dateX:%Y }}", at(2026, 9, 13, 0), "UTC").is_err());
    }

    #[test]
    fn unknown_timezone_is_rejected() {
        assert!(render("{{ date:%Y }}", at(2026, 9, 13, 0), "Mars/Olympus").is_err());
    }

    #[test]
    fn unterminated_token_is_rejected() {
        assert!(render("{{ date:%Y", at(2026, 9, 13, 0), "UTC").is_err());
    }

    #[test]
    fn a_dst_boundary_renders_the_local_date() {
        // Europe/Paris springs forward 2026-03-29 at 02:00 local. An instant just after
        // the transition must still render the local calendar date.
        let out =
            render("{{ date:%Y-%m-%d %H }}", at(2026, 3, 29, 1), "Europe/Paris").expect("renders");
        assert_eq!(out, "2026-03-29 03", "01:00Z is 03:00 local after the jump");
    }
}
