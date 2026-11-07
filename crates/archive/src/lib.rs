//! # meili-ingest archive
//!
//! Turns an archive into the files inside it, so each one can be ingested as if it had
//! been uploaded on its own. Three formats are read:
//!
//! * **zip**, the container users send most;
//! * **tar**, plain or wrapped in gzip (`.tar.gz`, `.tgz`);
//! * **gzip** around a single file (`report.pdf.gz` yields `report.pdf`).
//!
//! An archive found inside an archive is opened too, up to [`Limits::max_depth`] layers.
//! Office documents are zip files underneath, so a zip is only opened as a nested
//! archive when its name ends in `.zip`: a `.docx` inside an archive stays a file.
//!
//! Everything here is synchronous and CPU-bound: callers run [`walk`] off the async
//! runtime. The walk never touches the filesystem. Entry names are only ever used as
//! labels, after the unsafe ones (absolute, or climbing out with `..`) are skipped.
//!
//! ## Limits
//!
//! A small archive can expand enormously, so [`Limits`] bounds the walk:
//!
//! * `max_files` and `max_total_bytes` stop the whole walk with an error: ingesting
//!   only part of an archive would be a surprise;
//! * `max_file_bytes` and `max_depth` only skip the file or nested archive concerned,
//!   which is reported as [`Event::Skipped`].
//!
//! `max_total_bytes` counts what decompression produces (zip entries and gzip
//! streams). Tar entries are slices of data already counted, so they do not add to it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;
use std::io::{Cursor, Read};
use std::path::{Component, Path};

/// Canonical MIME type of a zip archive.
pub const MIME_ZIP: &str = "application/zip";
/// Canonical MIME type of a gzip stream.
pub const MIME_GZIP: &str = "application/gzip";
/// Canonical MIME type of a tar archive.
pub const MIME_TAR: &str = "application/x-tar";

/// An archive format this crate reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Zip.
    Zip,
    /// Tar.
    Tar,
    /// Gzip, around one file or around a tar.
    Gzip,
}

impl Format {
    /// The format named by a canonical MIME type.
    pub fn from_mime(mime: &str) -> Option<Self> {
        match mime {
            MIME_ZIP => Some(Self::Zip),
            MIME_TAR => Some(Self::Tar),
            MIME_GZIP => Some(Self::Gzip),
            _ => None,
        }
    }

    /// The format of `data`, from its magic bytes alone.
    pub fn sniff(data: &[u8]) -> Option<Self> {
        if data.starts_with(&[0x1f, 0x8b]) {
            Some(Self::Gzip)
        } else if data.starts_with(b"PK\x03\x04") || data.starts_with(b"PK\x05\x06") {
            Some(Self::Zip)
        } else if is_tar(data) {
            Some(Self::Tar)
        } else {
            None
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Zip => "zip",
            Self::Tar => "tar",
            Self::Gzip => "gzip",
        }
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// POSIX and GNU tar both put `ustar` at offset 257.
fn is_tar(data: &[u8]) -> bool {
    data.get(257..262) == Some(b"ustar")
}

/// Bounds on one walk. See the crate docs for which ones fail the walk and which skip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Most files one archive may yield, nested archives included.
    pub max_files: usize,
    /// Largest single file, uncompressed. Larger ones are skipped.
    pub max_file_bytes: u64,
    /// Most bytes decompression may produce over the whole walk.
    pub max_total_bytes: u64,
    /// Most archive layers opened, the outer archive included. An archive nested
    /// deeper is skipped.
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_files: 10_000,
            max_file_bytes: 512 * 1024 * 1024,
            max_total_bytes: 8 * 1024 * 1024 * 1024,
            max_depth: 3,
        }
    }
}

/// What the walk found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A file to ingest.
    File {
        /// Path inside the archive, `/`-separated, without a leading `/`. Files of a
        /// nested zip or tar are under the nested archive's own path
        /// (`data/inner.zip/a.pdf`).
        path: String,
        /// Its content.
        data: Vec<u8>,
    },
    /// An entry that will not be ingested, and why.
    Skipped {
        /// Path inside the archive.
        path: String,
        /// Why it was skipped.
        reason: String,
    },
}

/// Totals of a finished walk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// Files yielded.
    pub files: usize,
    /// Entries skipped (directories and system files such as `.DS_Store` are dropped
    /// silently and not counted).
    pub skipped: usize,
    /// Bytes decompression produced.
    pub decompressed_bytes: u64,
}

/// Why a walk stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveError {
    /// The archive cannot be read.
    #[error("not a readable {format} archive: {message}")]
    Corrupt {
        /// The format it was read as.
        format: Format,
        /// What went wrong.
        message: String,
    },
    /// More files than [`Limits::max_files`].
    #[error("the archive holds more than {0} files")]
    TooManyFiles(usize),
    /// Decompression produced more than [`Limits::max_total_bytes`].
    #[error("the archive expands to more than {0} bytes")]
    TooLarge(u64),
    /// A single gzip stream expands to more than [`Limits::max_file_bytes`].
    #[error("the file expands to more than {0} bytes")]
    FileTooLarge(u64),
    /// The sink asked to stop.
    #[error("{0}")]
    Stopped(String),
}

impl ArchiveError {
    /// Whether the error concerns only the archive being read, so a nested archive
    /// with this error can be skipped while the outer walk goes on.
    fn is_local(&self) -> bool {
        matches!(self, Self::Corrupt { .. } | Self::FileTooLarge(_))
    }
}

/// Walk `data`, read as `format`, and hand every file and skipped entry to `sink`.
///
/// `name` is the archive's own filename, used to name the file inside a lone gzip
/// stream (`report.pdf.gz` → `report.pdf`). The sink returns `Err` to stop the walk,
/// which then ends with [`ArchiveError::Stopped`].
pub fn walk(
    data: &[u8],
    format: Format,
    name: Option<&str>,
    limits: &Limits,
    sink: &mut dyn FnMut(Event) -> Result<(), String>,
) -> Result<Summary, ArchiveError> {
    let mut walker = Walker {
        limits,
        sink,
        summary: Summary::default(),
    };
    walker.archive(data, format, "", name, 1)?;
    Ok(walker.summary)
}

struct Walker<'a> {
    limits: &'a Limits,
    sink: &'a mut dyn FnMut(Event) -> Result<(), String>,
    summary: Summary,
}

impl Walker<'_> {
    /// Read one archive layer. `prefix` is prepended to its entry paths; `depth` is
    /// how many layers are open, this one included.
    fn archive(
        &mut self,
        data: &[u8],
        format: Format,
        prefix: &str,
        name: Option<&str>,
        depth: usize,
    ) -> Result<(), ArchiveError> {
        match format {
            Format::Zip => self.zip(data, prefix, depth),
            Format::Tar => self.tar(data, prefix, depth),
            Format::Gzip => self.gzip(data, prefix, name, depth),
        }
    }

    fn zip(&mut self, data: &[u8], prefix: &str, depth: usize) -> Result<(), ArchiveError> {
        let corrupt = |e: zip::result::ZipError| ArchiveError::Corrupt {
            format: Format::Zip,
            message: e.to_string(),
        };
        let mut archive = zip::ZipArchive::new(Cursor::new(data)).map_err(corrupt)?;
        for i in 0..archive.len() {
            // The raw view reads the header without decrypting or decompressing, so an
            // entry the reader cannot open can still be named in the skip report.
            let (raw_name, safe, is_dir, is_symlink, declared) = {
                let raw = archive.by_index_raw(i).map_err(corrupt)?;
                (
                    raw.name().to_string(),
                    raw.enclosed_name().map(|p| join_components(&p)),
                    raw.is_dir(),
                    raw.is_symlink(),
                    raw.size(),
                )
            };
            if is_dir {
                continue;
            }
            let Some(rel) = safe.filter(|p| !p.is_empty()) else {
                self.skip(format!("{prefix}{raw_name}"), "unsafe path")?;
                continue;
            };
            let path = format!("{prefix}{rel}");
            if is_noise(&path) {
                continue;
            }
            if is_symlink {
                self.skip(path, "symbolic link")?;
                continue;
            }
            if declared > self.limits.max_file_bytes {
                self.skip(path, &self.too_large())?;
                continue;
            }
            let mut file = match archive.by_index(i) {
                Ok(f) => f,
                Err(e) => {
                    self.skip(path, &unreadable(&e.to_string()))?;
                    continue;
                }
            };
            let data = match self.read_counted(&mut file) {
                Ok(Some(d)) => d,
                Ok(None) => {
                    self.skip(path, &self.too_large())?;
                    continue;
                }
                Err(Read2::Io(e)) => {
                    self.skip(path, &unreadable(&e))?;
                    continue;
                }
                Err(Read2::Fatal(e)) => return Err(e),
            };
            drop(file);
            self.file(path, data, depth)?;
        }
        Ok(())
    }

    fn tar(&mut self, data: &[u8], prefix: &str, depth: usize) -> Result<(), ArchiveError> {
        let corrupt = |e: std::io::Error| ArchiveError::Corrupt {
            format: Format::Tar,
            message: e.to_string(),
        };
        let mut archive = tar::Archive::new(Cursor::new(data));
        for entry in archive.entries().map_err(corrupt)? {
            let mut entry = entry.map_err(corrupt)?;
            let kind = entry.header().entry_type();
            let raw = entry.path_bytes();
            let raw_name = String::from_utf8_lossy(&raw).into_owned();
            if kind.is_dir() {
                continue;
            }
            let rel = entry
                .path()
                .ok()
                .and_then(|p| safe_relative(&p))
                .filter(|p| !p.is_empty());
            let Some(rel) = rel else {
                self.skip(format!("{prefix}{raw_name}"), "unsafe path")?;
                continue;
            };
            let path = format!("{prefix}{rel}");
            if is_noise(&path) {
                continue;
            }
            if kind.is_symlink() || kind.is_hard_link() {
                self.skip(path, "link")?;
                continue;
            }
            if !(kind.is_file() || kind.is_contiguous()) {
                // Devices, FIFOs and extension headers carry no content to ingest.
                continue;
            }
            if entry.size() > self.limits.max_file_bytes {
                self.skip(path, &self.too_large())?;
                continue;
            }
            let mut content = Vec::new();
            match (&mut entry)
                .take(self.limits.max_file_bytes.saturating_add(1))
                .read_to_end(&mut content)
            {
                Ok(_) if content.len() as u64 > self.limits.max_file_bytes => {
                    self.skip(path, &self.too_large())?;
                    continue;
                }
                Ok(_) => {}
                Err(e) => return Err(corrupt(e)),
            }
            self.file(path, content, depth)?;
        }
        Ok(())
    }

    fn gzip(
        &mut self,
        data: &[u8],
        prefix: &str,
        name: Option<&str>,
        depth: usize,
    ) -> Result<(), ArchiveError> {
        let mut decoder = flate2::read::MultiGzDecoder::new(data);
        let inner = match self.read_counted(&mut decoder) {
            Ok(Some(d)) => d,
            Ok(None) => return Err(ArchiveError::FileTooLarge(self.limits.max_file_bytes)),
            Err(Read2::Io(message)) => {
                return Err(ArchiveError::Corrupt {
                    format: Format::Gzip,
                    message,
                });
            }
            Err(Read2::Fatal(e)) => return Err(e),
        };
        // A .tar.gz is one archive, not two: its entries sit where the .tar.gz was.
        if is_tar(&inner) {
            return self.tar(&inner, prefix, depth);
        }
        let file_name = name.map(gunzipped_name).unwrap_or_else(|| "file".into());
        self.file(format!("{prefix}{file_name}"), inner, depth)
    }

    /// A file found at `depth`: open it when it is an archive, else hand it over.
    fn file(&mut self, path: String, data: Vec<u8>, depth: usize) -> Result<(), ArchiveError> {
        if data.is_empty() {
            return self.skip(path, "empty file");
        }
        if let Some(format) = nested_format(&data, &path) {
            if depth >= self.limits.max_depth {
                let reason = format!(
                    "archive nested more than {} levels deep",
                    self.limits.max_depth
                );
                return self.skip(path, &reason);
            }
            let (prefix, name) = match format {
                // A lone gzip stream holds one file, which takes the stream's place.
                Format::Gzip => match path.rsplit_once('/') {
                    Some((dir, base)) => (format!("{dir}/"), base.to_string()),
                    None => (String::new(), path.clone()),
                },
                Format::Zip | Format::Tar => (format!("{path}/"), basename(&path).to_string()),
            };
            return match self.archive(&data, format, &prefix, Some(&name), depth + 1) {
                Err(e) if e.is_local() => self.skip(path, &e.to_string()),
                other => other,
            };
        }
        if self.summary.files >= self.limits.max_files {
            return Err(ArchiveError::TooManyFiles(self.limits.max_files));
        }
        self.summary.files += 1;
        (self.sink)(Event::File { path, data }).map_err(ArchiveError::Stopped)
    }

    fn skip(&mut self, path: String, reason: &str) -> Result<(), ArchiveError> {
        self.summary.skipped += 1;
        (self.sink)(Event::Skipped {
            path,
            reason: reason.to_string(),
        })
        .map_err(ArchiveError::Stopped)
    }

    fn too_large(&self) -> String {
        format!("larger than {} bytes", self.limits.max_file_bytes)
    }

    /// Read a decompressing reader to the end, counting what it produces against the
    /// total. `Ok(None)` when the file alone is over [`Limits::max_file_bytes`].
    fn read_counted(&mut self, reader: &mut dyn Read) -> Result<Option<Vec<u8>>, Read2> {
        let left = self
            .limits
            .max_total_bytes
            .saturating_sub(self.summary.decompressed_bytes);
        let cap = self.limits.max_file_bytes.min(left);
        let mut out = Vec::new();
        reader
            .take(cap.saturating_add(1))
            .read_to_end(&mut out)
            .map_err(|e| Read2::Io(e.to_string()))?;
        let len = out.len() as u64;
        if len > cap {
            if len > left {
                return Err(Read2::Fatal(ArchiveError::TooLarge(
                    self.limits.max_total_bytes,
                )));
            }
            return Ok(None);
        }
        self.summary.decompressed_bytes += len;
        Ok(Some(out))
    }
}

/// Failure of [`Walker::read_counted`]: an unreadable entry, or a limit that ends the
/// walk.
enum Read2 {
    Io(String),
    Fatal(ArchiveError),
}

fn unreadable(e: &str) -> String {
    format!("unreadable: {e}")
}

/// The format of a file found inside an archive, when it is one to open.
fn nested_format(data: &[u8], path: &str) -> Option<Format> {
    match Format::sniff(data)? {
        // Office documents, EPUBs and JARs are zips too: only `.zip` is a folder.
        Format::Zip if !has_extension(path, "zip") => None,
        other => Some(other),
    }
}

fn has_extension(path: &str, ext: &str) -> bool {
    basename(path)
        .rsplit_once('.')
        .is_some_and(|(_, e)| e.eq_ignore_ascii_case(ext))
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// `report.pdf.gz` → `report.pdf`, `logs.tgz` → `logs.tar`, `blob` → `blob`.
fn gunzipped_name(name: &str) -> String {
    let base = basename(name);
    let lower = base.to_ascii_lowercase();
    if lower.ends_with(".tgz") {
        format!("{}.tar", &base[..base.len() - 4])
    } else if lower.ends_with(".gz") && base.len() > 3 {
        base[..base.len() - 3].to_string()
    } else {
        base.to_string()
    }
}

/// System files that archivers add and nobody means to ingest.
fn is_noise(path: &str) -> bool {
    let base = basename(path);
    path.split('/').any(|c| c == "__MACOSX")
        || base.starts_with("._")
        || matches!(base, ".DS_Store" | "Thumbs.db" | "desktop.ini")
}

/// `/`-joined normal components of an already safe path.
fn join_components(path: &Path) -> String {
    path.components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// `None` for absolute paths and paths that climb with `..`; `.` components dropped.
fn safe_relative(path: &Path) -> Option<String> {
    let mut parts = Vec::new();
    for c in path.components() {
        match c {
            Component::Normal(s) => parts.push(s.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (name, data) in entries {
            if name.ends_with('/') {
                w.add_directory(*name, opts).unwrap();
            } else {
                w.start_file(*name, opts).unwrap();
                w.write_all(data).unwrap();
            }
        }
        w.finish().unwrap().into_inner()
    }

    fn tar_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (name, data) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, name, *data).unwrap();
        }
        b.into_inner().unwrap()
    }

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn run(
        data: &[u8],
        format: Format,
        name: Option<&str>,
        limits: &Limits,
    ) -> (Result<Summary, ArchiveError>, Vec<Event>) {
        let mut events = Vec::new();
        let r = walk(data, format, name, limits, &mut |e| {
            events.push(e);
            Ok(())
        });
        (r, events)
    }

    fn files(events: &[Event]) -> Vec<(&str, &[u8])> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::File { path, data } => Some((path.as_str(), data.as_slice())),
                Event::Skipped { .. } => None,
            })
            .collect()
    }

    fn skipped(events: &[Event]) -> Vec<(&str, &str)> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::Skipped { path, reason } => Some((path.as_str(), reason.as_str())),
                Event::File { .. } => None,
            })
            .collect()
    }

    #[test]
    fn formats_come_from_mime_or_magic() {
        assert_eq!(Format::from_mime(MIME_ZIP), Some(Format::Zip));
        assert_eq!(Format::from_mime(MIME_TAR), Some(Format::Tar));
        assert_eq!(Format::from_mime(MIME_GZIP), Some(Format::Gzip));
        assert_eq!(Format::from_mime("application/pdf"), None);
        assert_eq!(Format::sniff(&zip_of(&[("a", b"x")])), Some(Format::Zip));
        assert_eq!(Format::sniff(&tar_of(&[("a", b"x")])), Some(Format::Tar));
        assert_eq!(Format::sniff(&gz(b"x")), Some(Format::Gzip));
        assert_eq!(Format::sniff(b"%PDF-1.7"), None);
    }

    #[test]
    fn a_zip_keeps_its_folders_and_drops_system_files() {
        let data = zip_of(&[
            ("reports/", b""),
            ("reports/eu/a.pdf", b"%PDF eu"),
            ("reports/us/a.pdf", b"%PDF us"),
            ("notes.md", b"# hi"),
            ("__MACOSX/reports/._a.pdf", b"junk"),
            ("reports/.DS_Store", b"junk"),
        ]);
        let (r, events) = run(&data, Format::Zip, Some("q3.zip"), &Limits::default());
        let summary = r.unwrap();
        assert_eq!(
            files(&events),
            vec![
                ("reports/eu/a.pdf", b"%PDF eu".as_slice()),
                ("reports/us/a.pdf", b"%PDF us".as_slice()),
                ("notes.md", b"# hi".as_slice()),
            ]
        );
        assert!(skipped(&events).is_empty(), "{events:?}");
        assert_eq!(summary.files, 3);
        assert_eq!(summary.skipped, 0);
        assert!(summary.decompressed_bytes > 0);
    }

    #[test]
    fn a_tar_gz_is_one_archive() {
        let data = gz(&tar_of(&[
            ("docs/a.txt", b"alpha"),
            ("b.csv", b"x,y\n1,2\n"),
        ]));
        let (r, events) = run(&data, Format::Gzip, Some("dump.tgz"), &Limits::default());
        r.unwrap();
        assert_eq!(
            files(&events),
            vec![
                ("docs/a.txt", b"alpha".as_slice()),
                ("b.csv", b"x,y\n1,2\n".as_slice())
            ]
        );
    }

    #[test]
    fn a_lone_gzip_file_loses_its_gz_suffix() {
        let (r, events) = run(
            &gz(b"%PDF report"),
            Format::Gzip,
            Some("report.pdf.gz"),
            &Limits::default(),
        );
        r.unwrap();
        assert_eq!(
            files(&events),
            vec![("report.pdf", b"%PDF report".as_slice())]
        );
        assert_eq!(gunzipped_name("logs.TGZ"), "logs.tar");
        assert_eq!(gunzipped_name("blob"), "blob");
        assert_eq!(gunzipped_name(".gz"), ".gz");
    }

    #[test]
    fn nested_archives_are_opened_under_their_own_path() {
        let inner = zip_of(&[("deep.txt", b"deep")]);
        let logs = gz(b"line\n");
        let data = zip_of(&[
            ("data/inner.zip", &inner),
            ("logs/app.log.gz", &logs),
            ("a.txt", b"top"),
        ]);
        let (r, events) = run(&data, Format::Zip, None, &Limits::default());
        r.unwrap();
        assert_eq!(
            files(&events),
            vec![
                ("data/inner.zip/deep.txt", b"deep".as_slice()),
                ("logs/app.log", b"line\n".as_slice()),
                ("a.txt", b"top".as_slice()),
            ]
        );
    }

    #[test]
    fn an_office_document_inside_an_archive_stays_a_file() {
        // A .docx is a zip: opening it would ingest its XML parts instead of the document.
        let docx = zip_of(&[("word/document.xml", b"<w:document/>")]);
        let data = zip_of(&[("contract.docx", &docx)]);
        let (r, events) = run(&data, Format::Zip, None, &Limits::default());
        r.unwrap();
        assert_eq!(files(&events), vec![("contract.docx", docx.as_slice())]);
    }

    #[test]
    fn archives_nested_too_deep_are_skipped() {
        let level3 = zip_of(&[("c.txt", b"c")]);
        let level2 = zip_of(&[("l3.zip", &level3), ("b.txt", b"b")]);
        let data = zip_of(&[("l2.zip", &level2), ("a.txt", b"a")]);
        let limits = Limits {
            max_depth: 2,
            ..Limits::default()
        };
        let (r, events) = run(&data, Format::Zip, None, &limits);
        r.unwrap();
        assert_eq!(
            files(&events),
            vec![
                ("l2.zip/b.txt", b"b".as_slice()),
                ("a.txt", b"a".as_slice())
            ]
        );
        assert_eq!(
            skipped(&events),
            vec![("l2.zip/l3.zip", "archive nested more than 2 levels deep")]
        );
    }

    #[test]
    fn a_corrupt_nested_archive_is_skipped_not_fatal() {
        let data = zip_of(&[("broken.tar.gz", &[0x1f, 0x8b, 1, 2, 3]), ("ok.txt", b"ok")]);
        let (r, events) = run(&data, Format::Zip, None, &Limits::default());
        r.unwrap();
        assert_eq!(files(&events), vec![("ok.txt", b"ok".as_slice())]);
        let s = skipped(&events);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].0, "broken.tar.gz");
        assert!(s[0].1.starts_with("not a readable gzip archive"), "{s:?}");
    }

    #[test]
    fn a_corrupt_outer_archive_is_an_error() {
        let (r, _) = run(b"PK\x03\x04garbage", Format::Zip, None, &Limits::default());
        assert!(matches!(
            r,
            Err(ArchiveError::Corrupt {
                format: Format::Zip,
                ..
            })
        ));
    }

    #[test]
    fn unsafe_and_empty_entries_are_skipped() {
        let data = zip_of(&[("../evil.txt", b"x"), ("empty.txt", b""), ("ok.txt", b"ok")]);
        let (r, events) = run(&data, Format::Zip, None, &Limits::default());
        r.unwrap();
        assert_eq!(files(&events), vec![("ok.txt", b"ok".as_slice())]);
        assert_eq!(
            skipped(&events),
            vec![("../evil.txt", "unsafe path"), ("empty.txt", "empty file")]
        );
    }

    #[test]
    fn tar_links_and_climbing_paths_are_skipped() {
        let mut b = tar::Builder::new(Vec::new());
        let mut link = tar::Header::new_gnu();
        link.set_entry_type(tar::EntryType::Symlink);
        link.set_size(0);
        b.append_link(&mut link, "passwd", "/etc/passwd").unwrap();
        // `Builder` refuses `..`, so write the name into the raw header ourselves.
        let mut evil = tar::Header::new_old();
        evil.as_old_mut().name[..10].copy_from_slice(b"../x.txt\0\0");
        evil.set_size(1);
        evil.set_mode(0o644);
        evil.set_cksum();
        b.append(&evil, &b"x"[..]).unwrap();
        let mut ok = tar::Header::new_gnu();
        ok.set_size(2);
        ok.set_cksum();
        b.append_data(&mut ok, "ok.txt", &b"ok"[..]).unwrap();
        let data = b.into_inner().unwrap();

        let (r, events) = run(&data, Format::Tar, None, &Limits::default());
        r.unwrap();
        assert_eq!(files(&events), vec![("ok.txt", b"ok".as_slice())]);
        assert_eq!(
            skipped(&events),
            vec![("passwd", "link"), ("../x.txt", "unsafe path")]
        );
    }

    #[test]
    fn a_file_over_the_size_limit_is_skipped() {
        let data = zip_of(&[("big.txt", &[b'a'; 4096]), ("small.txt", b"s")]);
        let limits = Limits {
            max_file_bytes: 1024,
            ..Limits::default()
        };
        let (r, events) = run(&data, Format::Zip, None, &limits);
        r.unwrap();
        assert_eq!(files(&events), vec![("small.txt", b"s".as_slice())]);
        assert_eq!(
            skipped(&events),
            vec![("big.txt", "larger than 1024 bytes")]
        );
    }

    #[test]
    fn a_zip_bomb_stops_at_the_total_limit() {
        // Highly compressible: a few KiB on the wire, 3 MiB once inflated.
        let mib = vec![0u8; 1024 * 1024];
        let data = zip_of(&[("a", &mib), ("b", &mib), ("c", &mib)]);
        assert!(data.len() < 64 * 1024);
        let limits = Limits {
            max_total_bytes: 2 * 1024 * 1024 + 10,
            ..Limits::default()
        };
        let (r, events) = run(&data, Format::Zip, None, &limits);
        assert_eq!(r, Err(ArchiveError::TooLarge(2 * 1024 * 1024 + 10)));
        assert_eq!(files(&events).len(), 2, "the first two fit");
    }

    #[test]
    fn a_gzip_bomb_is_stopped_while_inflating() {
        let data = gz(&vec![0u8; 4 * 1024 * 1024]);
        let limits = Limits {
            max_file_bytes: 1024 * 1024,
            ..Limits::default()
        };
        let (r, events) = run(&data, Format::Gzip, Some("x.gz"), &limits);
        assert_eq!(r, Err(ArchiveError::FileTooLarge(1024 * 1024)));
        assert!(events.is_empty());
    }

    #[test]
    fn too_many_files_is_an_error() {
        let data = zip_of(&[("a", b"1"), ("b", b"2"), ("c", b"3")]);
        let limits = Limits {
            max_files: 2,
            ..Limits::default()
        };
        let (r, events) = run(&data, Format::Zip, None, &limits);
        assert_eq!(r, Err(ArchiveError::TooManyFiles(2)));
        assert_eq!(files(&events).len(), 2);
    }

    #[test]
    fn the_sink_can_stop_the_walk() {
        let data = zip_of(&[("a", b"1"), ("b", b"2")]);
        let mut seen = 0;
        let r = walk(&data, Format::Zip, None, &Limits::default(), &mut |_| {
            seen += 1;
            Err("receiver gone".into())
        });
        assert_eq!(r, Err(ArchiveError::Stopped("receiver gone".into())));
        assert_eq!(seen, 1);
    }
}
