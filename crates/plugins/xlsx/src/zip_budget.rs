//! Zip-bomb guard: bound what a zipped workbook (`.xlsx`, `.xlsm`, `.xlsb`, `.ods`)
//! inflates to before `calamine` reads it.
//!
//! Uploads arrive from untrusted tenants and deflate reaches ratios of ~1000:1, so a
//! small file can expand to gigabytes and exhaust a worker. `calamine` reads its
//! parts itself (shared strings and `.ods` content whole, sized from the declared
//! entry size), so the plugin cannot cap those reads. Instead every entry is
//! inflated once into a sink, through a capped reader, before the parser runs.
//! Deflate is deterministic, so a package that passes this check cannot inflate to
//! more when the parser reads it. The same `zip` crate (same features) does both
//! reads, so an entry this check cannot open cannot be opened by the parser either.
//!
//! The docx extractor carries the same guard with its own limits.

use std::io::{Cursor, Read};

use meili_ingest_plugin_sdk::PluginError;
use zip::ZipArchive;

/// How an entry counts against the [`Limits`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Part {
    /// Loaded whole and parsed (XML): bounded by [`Limits::part`] and the total.
    Parsed,
    /// Copied as-is (media): bounded by the total only.
    Copied,
}

/// Decompression limits for one archive.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// Largest inflated size of one [`Part::Parsed`] entry.
    pub part: u64,
    /// Largest inflated size of all entries together.
    pub total: u64,
    /// Classifies an entry by its name.
    pub classify: fn(&str) -> Part,
}

/// Refuse the archive in `data` if any entry, or all of them together, inflate past
/// `limits`. Bytes that are not a zip archive pass: the parser reports them.
pub(crate) fn check(data: &[u8], format: &str, limits: &Limits) -> Result<(), PluginError> {
    let Ok(mut archive) = ZipArchive::new(Cursor::new(data)) else {
        return Ok(());
    };
    let mut total: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];
    for index in 0..archive.len() {
        // An entry the `zip` crate cannot open (unsupported method, bad header) is
        // equally unreadable for the parser, which fails or skips it.
        let Ok(mut entry) = archive.by_index(index) else {
            continue;
        };
        let name = entry.name().to_owned();
        let part_cap = match (limits.classify)(&name) {
            Part::Parsed => limits.part,
            Part::Copied => u64::MAX,
        };
        let remaining = limits.total - total;

        // The declared size first: cheap, rejects an honest bomb before any work, and
        // the parser reserves that much memory up front.
        let declared = entry.size();
        if declared > part_cap {
            return Err(PluginError::NonRetryable(format!(
                "{format} part `{name}` declares {declared} bytes, above the {part_cap} byte \
                 limit; refusing to decompress (possible zip bomb)"
            )));
        }
        if declared > remaining {
            return Err(over_budget(format, limits.total));
        }

        // Then the actual inflated size, which a forged header can understate.
        // Reading one byte past the cap tells an overflow from an exact fit.
        let cap = part_cap.min(remaining);
        let mut reader = (&mut entry).take(cap.saturating_add(1));
        let mut read: u64 = 0;
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => read += n as u64,
                // A corrupt stream stops the parser at the same point.
                Err(_) => break,
            }
        }
        if read > cap {
            return Err(if read > part_cap {
                PluginError::NonRetryable(format!(
                    "{format} part `{name}` inflates past the {part_cap} byte limit; \
                     refusing to continue (possible zip bomb)"
                ))
            } else {
                over_budget(format, limits.total)
            });
        }
        total += read;
    }
    Ok(())
}

fn over_budget(format: &str, cap: u64) -> PluginError {
    PluginError::NonRetryable(format!(
        "{format} file decompresses past the {cap} byte limit for one file; \
         refusing to continue (possible zip bomb)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    fn media_is_copied(name: &str) -> Part {
        if name.starts_with("media/") {
            Part::Copied
        } else {
            Part::Parsed
        }
    }

    const LIMITS: Limits = Limits {
        part: 1000,
        total: 3000,
        classify: media_is_copied,
    };

    fn archive(entries: &[(&str, usize)]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, size) in entries {
            writer
                .start_file(*name, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(&vec![b'a'; *size]).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn message(result: Result<(), PluginError>) -> String {
        match result {
            Err(PluginError::NonRetryable(m)) => m,
            other => panic!("expected a NonRetryable error, got {other:?}"),
        }
    }

    #[test]
    fn archives_within_the_limits_pass() {
        let data = archive(&[("a.xml", 1000), ("b.xml", 1000), ("media/x.png", 1000)]);
        check(&data, "xlsx", &LIMITS).unwrap();
    }

    #[test]
    fn bytes_that_are_not_a_zip_pass() {
        check(b"PK\x03\x04 not a zip", "xlsx", &LIMITS).unwrap();
    }

    #[test]
    fn an_oversized_parsed_part_is_refused() {
        let m = message(check(&archive(&[("a.xml", 1001)]), "xlsx", &LIMITS));
        assert!(m.contains("`a.xml`") && m.contains("zip bomb"), "{m}");
    }

    #[test]
    fn copied_parts_only_count_towards_the_total() {
        check(&archive(&[("media/x.png", 2500)]), "xlsx", &LIMITS).unwrap();
        let data = archive(&[("media/x.png", 2500), ("a.xml", 600)]);
        let m = message(check(&data, "xlsx", &LIMITS));
        assert!(m.contains("3000 byte limit for one file"), "{m}");
    }

    #[test]
    fn a_header_that_understates_the_size_is_caught_while_inflating() {
        let mut data = archive(&[("a.xml", 5000)]);
        forge_uncompressed_size(&mut data, 10);
        let m = message(check(&data, "xlsx", &LIMITS));
        assert!(m.contains("inflates past the 1000 byte limit"), "{m}");
    }

    /// Rewrite the uncompressed size of every entry, in the local headers and the
    /// central directory, to `size`.
    fn forge_uncompressed_size(data: &mut [u8], size: u32) {
        let mut patched = 0;
        for at in 0..data.len().saturating_sub(4) {
            let offset = match &data[at..at + 4] {
                [0x50, 0x4b, 0x03, 0x04] => 22,
                [0x50, 0x4b, 0x01, 0x02] => 24,
                _ => continue,
            };
            data[at + offset..at + offset + 4].copy_from_slice(&size.to_le_bytes());
            patched += 1;
        }
        assert_eq!(patched, 2, "one local header and one central entry");
    }
}
