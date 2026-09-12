//! `video_audio_extractor`: pull the audio out of a video (or re-encode an audio
//! file) as 16 kHz mono 16-bit WAV, ready for a speech-to-text step.
//!
//! # Why no ffmpeg
//!
//! SPEC §15 item 22 assumed an ffmpeg-based gRPC container. This implementation is
//! 100% Rust — [`symphonia`] demuxes and decodes, [`rubato`] resamples and [`hound`]
//! encodes — so the media path needs no subprocess, no shared libraries and no extra
//! container. The trade-off is codec coverage: symphonia handles the containers and
//! codecs listed in [`SUPPORTED`], which covers the overwhelming majority of MP4,
//! WebM, MKV and OGG files, but not AC-3/E-AC-3 or Opus-in-MP4. A file symphonia
//! cannot decode fails with a non-retryable error naming the codec, so the operator
//! knows to transcode it or register a gRPC plugin under the same name instead.
//!
//! # Output
//!
//! One [`PluginOutput::Bytes`] holding a RIFF/WAV file: 16-bit signed PCM, mono,
//! 16 kHz by default. That is what speech-to-text endpoints want, and it keeps the
//! payload roughly 5.5× smaller than 44.1 kHz stereo, which matters because the next
//! step uploads it over HTTP.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use meili_ingest_plugin_sdk::prelude::*;
use rubato::audioadapter_buffers::owned::InterleavedOwned;
use rubato::{Fft, FixedSync, Resampler};
use serde::{Deserialize, Serialize};
use symphonia::core::audio::GenericAudioBufferRef;
use symphonia::core::codecs::CodecParameters;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, TrackType};
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "video_audio_extractor";

/// Default output sample rate: what speech-to-text models expect.
pub const DEFAULT_SAMPLE_RATE: u32 = 16_000;

/// Containers and codecs this plugin can decode, for error messages and docs.
pub const SUPPORTED: &str = "containers MP4/MOV, MKV/WebM, OGG, WAV, FLAC, AIFF, CAF \
                             and codecs AAC-LC, MP1/MP2/MP3, ALAC, FLAC, Vorbis, PCM, ADPCM";

/// Check the cancellation flag this often (in decoded packets).
const CANCEL_CHECK_EVERY: usize = 64;

/// Resampler chunk size. 1024 frames balances FFT cost against latency.
const RESAMPLE_CHUNK: usize = 1024;

/// Configuration accepted in a pipeline step's `config:` block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct VideoAudioConfig {
    /// Output sample rate in Hz.
    pub sample_rate: u32,
    /// Downmix every channel into one. When false the source channel count is kept.
    pub mono: bool,
    /// Stop decoding after this many seconds of audio.
    pub max_duration_secs: Option<u64>,
    /// Decode this track id instead of the container's default audio track.
    pub track: Option<u32>,
}

impl Default for VideoAudioConfig {
    fn default() -> Self {
        Self {
            sample_rate: DEFAULT_SAMPLE_RATE,
            mono: true,
            max_duration_secs: None,
            track: None,
        }
    }
}

impl VideoAudioConfig {
    fn parse(value: serde_json::Value) -> Result<Self, PluginError> {
        let cfg: Self = if value.is_null() {
            Self::default()
        } else {
            serde_json::from_value(value).map_err(PluginError::invalid_config)?
        };
        if cfg.sample_rate < 1_000 || cfg.sample_rate > 384_000 {
            return Err(PluginError::invalid_config(format!(
                "sample_rate must be between 1000 and 384000 Hz, got {}",
                cfg.sample_rate
            )));
        }
        if cfg.max_duration_secs == Some(0) {
            return Err(PluginError::invalid_config(
                "max_duration_secs must be greater than zero",
            ));
        }
        Ok(cfg)
    }
}

/// Extracts a video's audio track as WAV.
#[derive(Debug, Clone, Copy, Default)]
pub struct VideoAudioExtractorPlugin;

impl VideoAudioExtractorPlugin {
    /// Build the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// What the blocking decode returns to the async side.
#[derive(Debug)]
struct Decoded {
    /// Interleaved samples at `channels` × `frames`.
    samples: Vec<f32>,
    channels: usize,
    rate: u32,
    /// Packets skipped because the decoder called them corrupt.
    skipped: usize,
}

#[async_trait]
impl Plugin for VideoAudioExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Decodes the audio track of a video or audio file and re-encodes it as \
                 16 kHz mono 16-bit WAV for speech-to-text. Pure Rust (symphonia), no ffmpeg.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Bytes)
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "sample_rate": {
                        "type": "integer",
                        "minimum": 1000,
                        "maximum": 384000,
                        "default": DEFAULT_SAMPLE_RATE,
                        "description": "Output sample rate in Hz."
                    },
                    "mono": {
                        "type": "boolean",
                        "default": true,
                        "description": "Downmix all channels to one."
                    },
                    "max_duration_secs": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "default": null,
                        "description": "Stop decoding after this many seconds. Unlimited when unset."
                    },
                    "track": {
                        "type": ["integer", "null"],
                        "default": null,
                        "description": "Track id to decode instead of the default audio track."
                    }
                }
            }))
            .content_types([
                "video/mp4",
                "video/quicktime",
                "video/webm",
                "video/x-matroska",
                "audio/mpeg",
                "audio/mp4",
                "audio/wav",
                "audio/ogg",
                "audio/flac",
            ])
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg = VideoAudioConfig::parse(config)?;
        let blob = input.into_bytes()?;
        let mime = blob
            .mime
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if !(mime.starts_with("video/") || mime.starts_with("audio/")) {
            return Err(PluginError::invalid_input(format!(
                "{NAME} accepts audio/* or video/* content, got {:?}",
                blob.mime
            )));
        }
        if blob.data.is_empty() {
            return Err(PluginError::invalid_input("received an empty media file"));
        }
        ctx.check_cancelled()?;

        let stem = file_stem(blob.filename.as_deref());
        let extension = extension(blob.filename.as_deref());
        ctx.heartbeat(format!("decoding {} bytes of {mime}", blob.data.len()));

        // Demuxing, decoding and resampling are CPU-bound and can run for minutes on a
        // long recording, so they happen on a blocking thread. The closure cannot touch
        // `ctx`, so it polls the shared cancellation flag instead.
        let cancelled = ctx.cancellation_flag();
        let data = blob.data;
        let mime_for_hint = mime.clone();
        let cfg_for_decode = cfg.clone();
        let decoded = run_blocking(move || {
            decode(
                data,
                &mime_for_hint,
                extension.as_deref(),
                &cfg_for_decode,
                &cancelled,
            )
        })
        .await??;

        if decoded.skipped > 0 {
            tracing::warn!(
                plugin = NAME,
                skipped_packets = decoded.skipped,
                "skipped corrupt packets while decoding"
            );
        }
        ctx.check_cancelled()?;

        let source_frames = decoded.samples.len() / decoded.channels.max(1);
        let target_channels = if cfg.mono { 1 } else { decoded.channels };
        ctx.heartbeat(format!(
            "decoded {:.1}s at {} Hz, resampling to {} Hz",
            source_frames as f64 / decoded.rate.max(1) as f64,
            decoded.rate,
            cfg.sample_rate
        ));

        let cancelled = ctx.cancellation_flag();
        let sample_rate = cfg.sample_rate;
        let mono = cfg.mono;
        let wav = run_blocking(move || {
            let mixed = if mono {
                downmix_to_mono(&decoded.samples, decoded.channels)
            } else {
                decoded.samples
            };
            if cancelled.load(Ordering::Relaxed) {
                return Err(PluginError::Cancelled);
            }
            let resampled = resample(mixed, target_channels, decoded.rate, sample_rate)?;
            encode_wav(&resampled, target_channels as u16, sample_rate)
        })
        .await??;

        ctx.heartbeat(format!("encoded {} bytes of wav", wav.len()));
        tracing::info!(
            plugin = NAME,
            source_rate = decoded.rate,
            source_channels = decoded.channels,
            target_rate = sample_rate,
            target_channels,
            wav_bytes = wav.len(),
            "extracted audio"
        );
        Ok(PluginOutput::Bytes(Blob::new(
            wav,
            "audio/wav",
            Some(format!("{stem}.wav")),
        )))
    }
}

/// Demux and decode every audio packet into interleaved f32 samples.
fn decode(
    data: Vec<u8>,
    mime: &str,
    extension: Option<&str>,
    cfg: &VideoAudioConfig,
    cancelled: &Arc<AtomicBool>,
) -> Result<Decoded, PluginError> {
    let mss = MediaSourceStream::new(Box::new(Cursor::new(data)), Default::default());
    let mut hint = Hint::new();
    hint.mime_type(mime);
    if let Some(ext) = extension {
        hint.with_extension(ext);
    }

    let mut format = symphonia::default::get_probe()
        .probe(
            &hint,
            mss,
            FormatOptions::default(),
            MetadataOptions::default(),
        )
        .map_err(|e| {
            PluginError::non_retryable(format!(
                "cannot read this media container ({e}); {NAME} supports {SUPPORTED}"
            ))
        })?;

    let track = match cfg.track {
        Some(id) => format.tracks().iter().find(|t| t.id == id).ok_or_else(|| {
            PluginError::non_retryable(format!("no track with id {id} in this file"))
        })?,
        None => format
            .default_track(TrackType::Audio)
            .ok_or_else(|| PluginError::non_retryable("this file has no audio track"))?,
    };
    let track_id = track.id;
    let audio_params = match &track.codec_params {
        Some(CodecParameters::Audio(p)) => p.clone(),
        _ => {
            return Err(PluginError::non_retryable(
                "the selected track has no audio codec parameters; the container may be truncated",
            ));
        }
    };

    let mut decoder = symphonia::default::get_codecs()
        .make_audio_decoder(&audio_params, &AudioDecoderOptions::default())
        .map_err(|e| {
            PluginError::non_retryable(format!(
                "no decoder for this audio codec ({e}); {NAME} supports {SUPPORTED} — \
                 transcode the file or register a gRPC plugin named {NAME}"
            ))
        })?;

    let mut samples: Vec<f32> = Vec::new();
    let mut interleaved: Vec<f32> = Vec::new();
    let mut channels = 0usize;
    let mut rate = 0u32;
    let mut skipped = 0usize;
    let mut packets = 0usize;
    let max_frames = cfg
        .max_duration_secs
        .map(|secs| secs.saturating_mul(u64::from(audio_params.sample_rate.unwrap_or(48_000))));

    loop {
        if packets.is_multiple_of(CANCEL_CHECK_EVERY) && cancelled.load(Ordering::Relaxed) {
            return Err(PluginError::Cancelled);
        }
        packets += 1;

        let packet = match format.next_packet() {
            Ok(Some(p)) => p,
            Ok(None) => break,
            // A truncated file still yields whatever decoded cleanly.
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                return Err(PluginError::non_retryable(format!(
                    "error reading the media stream: {e}"
                )));
            }
        };
        if packet.track_id != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(buf) => {
                append_interleaved(&buf, &mut interleaved);
                let spec = buf.spec();
                if channels == 0 {
                    channels = spec.channels().count();
                    rate = spec.rate();
                }
                samples.append(&mut interleaved);
            }
            // Corrupt packets are normal in streamed media: skip and keep going.
            Err(SymphoniaError::DecodeError(_)) => skipped += 1,
            Err(SymphoniaError::ResetRequired) => {
                decoder.reset();
                skipped += 1;
            }
            Err(e) => {
                return Err(PluginError::non_retryable(format!(
                    "audio decoding failed: {e}"
                )));
            }
        }

        if let Some(max) = max_frames
            && channels > 0
            && (samples.len() / channels) as u64 >= max
        {
            break;
        }
    }

    if samples.is_empty() || channels == 0 || rate == 0 {
        return Err(PluginError::non_retryable(format!(
            "decoded no audio from this file ({skipped} packets were corrupt); \
             {NAME} supports {SUPPORTED}"
        )));
    }

    // Honour max_duration exactly (the loop stops at a packet boundary).
    if let Some(max) = cfg.max_duration_secs {
        let keep = (max.saturating_mul(u64::from(rate)) as usize).saturating_mul(channels);
        if samples.len() > keep {
            samples.truncate(keep);
        }
    }

    Ok(Decoded {
        samples,
        channels,
        rate,
        skipped,
    })
}

/// Copy any sample format out of a decoded buffer as interleaved f32.
fn append_interleaved(buf: &GenericAudioBufferRef<'_>, out: &mut Vec<f32>) {
    out.clear();
    buf.copy_to_vec_interleaved(out);
}

/// Average every channel into one.
fn downmix_to_mono(samples: &[f32], channels: usize) -> Vec<f32> {
    if channels <= 1 {
        return samples.to_vec();
    }
    let frames = samples.len() / channels;
    let mut mono = Vec::with_capacity(frames);
    for frame in 0..frames {
        let start = frame * channels;
        let sum: f32 = samples[start..start + channels].iter().sum();
        mono.push(sum / channels as f32);
    }
    mono
}

/// Band-limited resampling with rubato's FFT resampler. A no-op when the rates match.
fn resample(
    samples: Vec<f32>,
    channels: usize,
    from_rate: u32,
    to_rate: u32,
) -> Result<Vec<f32>, PluginError> {
    if from_rate == to_rate || samples.is_empty() {
        return Ok(samples);
    }
    let frames = samples.len() / channels.max(1);
    let input_data: Vec<f64> = samples.into_iter().map(f64::from).collect();
    let input = InterleavedOwned::new_from(input_data, channels, frames)
        .map_err(|e| PluginError::non_retryable(format!("cannot wrap decoded audio: {e}")))?;

    let mut resampler = Fft::<f64>::new(
        from_rate as usize,
        to_rate as usize,
        RESAMPLE_CHUNK,
        channels,
        FixedSync::Both,
    )
    .map_err(|e| {
        PluginError::non_retryable(format!(
            "cannot resample {from_rate} Hz to {to_rate} Hz: {e}"
        ))
    })?;

    let needed = resampler.process_all_needed_output_len(frames);
    let mut output = InterleavedOwned::<f64>::new(0.0, channels, needed);
    let (_consumed, produced) = resampler
        .process_all_into_buffer(&input, &mut output, frames, None)
        .map_err(|e| PluginError::non_retryable(format!("resampling failed: {e}")))?;

    let mut data = output.take_data();
    data.truncate(produced * channels);
    Ok(data.into_iter().map(|v| v as f32).collect())
}

/// Encode interleaved f32 in [-1, 1] as 16-bit PCM WAV.
fn encode_wav(samples: &[f32], channels: u16, rate: u32) -> Result<Vec<u8>, PluginError> {
    let spec = hound::WavSpec {
        channels,
        sample_rate: rate,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut writer = hound::WavWriter::new(&mut cursor, spec)
            .map_err(|e| PluginError::non_retryable(format!("cannot start wav encoding: {e}")))?;
        for &s in samples {
            let clamped = (s.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
            writer.write_sample(clamped).map_err(|e| {
                PluginError::non_retryable(format!("cannot write wav samples: {e}"))
            })?;
        }
        writer
            .finalize()
            .map_err(|e| PluginError::non_retryable(format!("cannot finalize wav: {e}")))?;
    }
    Ok(cursor.into_inner())
}

/// Filename without directories or extension, sanitized for use in an id.
fn file_stem(filename: Option<&str>) -> String {
    let base = filename
        .map(|f| f.rsplit(['/', '\\']).next().unwrap_or(f))
        .map(|f| f.rsplit_once('.').map(|(s, _)| s).unwrap_or(f))
        .filter(|s| !s.is_empty())
        .unwrap_or("audio");
    meili_ingest_plugin_sdk::sanitize_id(base)
}

/// Lower-case file extension, used as a probe hint.
fn extension(filename: Option<&str>) -> Option<String> {
    filename
        .and_then(|f| f.rsplit_once('.'))
        .map(|(_, ext)| ext.to_ascii_lowercase())
        .filter(|e| !e.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthesise a WAV so the tests need no checked-in binaries. Symphonia decodes
    /// WAV/PCM, so this exercises the whole demux → decode → downmix → resample →
    /// encode path.
    fn wav(freq: f64, secs: f64, rate: u32, channels: u16) -> Vec<u8> {
        let spec = hound::WavSpec {
            channels,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut w = hound::WavWriter::new(&mut cursor, spec).expect("writer");
            let frames = (rate as f64 * secs) as usize;
            for i in 0..frames {
                let t = i as f64 / rate as f64;
                let v =
                    (0.6 * (2.0 * std::f64::consts::PI * freq * t).sin() * i16::MAX as f64) as i16;
                for _ in 0..channels {
                    w.write_sample(v).expect("sample");
                }
            }
            w.finalize().expect("finalize");
        }
        cursor.into_inner()
    }

    fn read_wav(bytes: &[u8]) -> (hound::WavSpec, Vec<f32>) {
        let reader = hound::WavReader::new(Cursor::new(bytes)).expect("readable wav");
        let spec = reader.spec();
        let samples: Vec<f32> = reader
            .into_samples::<i16>()
            .map(|s| s.expect("sample") as f32 / i16::MAX as f32)
            .collect();
        (spec, samples)
    }

    /// Dominant frequency of a mono signal, by Goertzel over candidate bins.
    fn dominant_freq(samples: &[f32], rate: u32) -> f64 {
        let mut best = (0.0f64, 0.0f64);
        let mut f = 100.0f64;
        while f < (rate as f64 / 2.0) - 50.0 {
            let w = 2.0 * std::f64::consts::PI * f / rate as f64;
            let coeff = 2.0 * w.cos();
            let (mut s1, mut s2) = (0.0f64, 0.0f64);
            for &x in samples {
                let s0 = f64::from(x) + coeff * s1 - s2;
                s2 = s1;
                s1 = s0;
            }
            let power = s1 * s1 + s2 * s2 - coeff * s1 * s2;
            if power > best.1 {
                best = (f, power);
            }
            f += 5.0;
        }
        best.0
    }

    async fn run(bytes: Vec<u8>, mime: &str, name: &str, cfg: serde_json::Value) -> PluginOutput {
        VideoAudioExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(bytes, mime, Some(name.to_string()))),
                cfg,
            )
            .await
            .expect("extraction succeeds")
    }

    #[tokio::test]
    async fn stereo_44100_becomes_mono_16000_preserving_the_tone() {
        let out = run(
            wav(440.0, 0.5, 44_100, 2),
            "audio/wav",
            "clip.wav",
            serde_json::json!({}),
        )
        .await;
        let bytes = match out {
            PluginOutput::Bytes(b) => {
                assert_eq!(b.mime, "audio/wav");
                assert_eq!(b.filename.as_deref(), Some("clip.wav"));
                b.data
            }
            other => panic!("expected bytes, got {other:?}"),
        };
        let (spec, samples) = read_wav(&bytes);
        assert_eq!(spec.channels, 1);
        assert_eq!(spec.sample_rate, 16_000);
        assert_eq!(spec.bits_per_sample, 16);

        // Duration survives the rate change (allow a few ms of resampler edge effects).
        let secs = samples.len() as f64 / 16_000.0;
        assert!((secs - 0.5).abs() < 0.02, "duration drifted: {secs}s");

        // And so does the signal: still a 440 Hz tone, not aliased noise.
        let peak = dominant_freq(&samples, 16_000);
        assert!(
            (peak - 440.0).abs() <= 10.0,
            "dominant frequency was {peak} Hz"
        );
    }

    #[tokio::test]
    async fn a_tone_above_the_new_nyquist_is_filtered_not_aliased() {
        // 15 kHz at 44.1 kHz cannot exist at 16 kHz (Nyquist 8 kHz). A naive
        // decimator would fold it down to ~1 kHz; a band-limited resampler must not.
        let out = run(
            wav(15_000.0, 0.4, 44_100, 1),
            "audio/wav",
            "hf.wav",
            serde_json::json!({}),
        )
        .await;
        let bytes = match out {
            PluginOutput::Bytes(b) => b.data,
            other => panic!("expected bytes, got {other:?}"),
        };
        let (_, samples) = read_wav(&bytes);
        let energy: f64 = samples
            .iter()
            .map(|s| f64::from(*s) * f64::from(*s))
            .sum::<f64>()
            / samples.len().max(1) as f64;
        assert!(
            energy < 0.01,
            "out-of-band tone leaked through with energy {energy}"
        );
    }

    #[tokio::test]
    async fn already_16k_mono_passes_through_untouched() {
        let source = wav(440.0, 0.25, 16_000, 1);
        let out = run(
            source.clone(),
            "audio/wav",
            "same.wav",
            serde_json::json!({}),
        )
        .await;
        let bytes = match out {
            PluginOutput::Bytes(b) => b.data,
            other => panic!("expected bytes, got {other:?}"),
        };
        let (spec, samples) = read_wav(&bytes);
        let (_, original) = read_wav(&source);
        assert_eq!(spec.sample_rate, 16_000);
        assert_eq!(spec.channels, 1);
        assert_eq!(samples.len(), original.len());
        for (a, b) in samples.iter().zip(original.iter()) {
            assert!(
                (a - b).abs() < 1e-3,
                "resampling altered a pass-through file"
            );
        }
    }

    #[tokio::test]
    async fn sample_rate_and_mono_are_configurable() {
        let out = run(
            wav(300.0, 0.3, 44_100, 2),
            "audio/wav",
            "cfg.wav",
            serde_json::json!({"sample_rate": 8000, "mono": false}),
        )
        .await;
        let bytes = match out {
            PluginOutput::Bytes(b) => b.data,
            other => panic!("expected bytes, got {other:?}"),
        };
        let (spec, _) = read_wav(&bytes);
        assert_eq!(spec.sample_rate, 8_000);
        assert_eq!(spec.channels, 2);
    }

    #[tokio::test]
    async fn max_duration_truncates() {
        let out = run(
            wav(440.0, 2.0, 44_100, 1),
            "audio/wav",
            "long.wav",
            serde_json::json!({"max_duration_secs": 1}),
        )
        .await;
        let bytes = match out {
            PluginOutput::Bytes(b) => b.data,
            other => panic!("expected bytes, got {other:?}"),
        };
        let (_, samples) = read_wav(&bytes);
        let secs = samples.len() as f64 / 16_000.0;
        assert!(secs <= 1.05, "expected about 1s, got {secs}s");
        assert!(secs > 0.9, "truncated too aggressively: {secs}s");
    }

    #[tokio::test]
    async fn undecodable_bytes_name_the_supported_formats() {
        let err = VideoAudioExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(
                    vec![0x00, 0x01, 0x02, 0x03, 0xff, 0xfe],
                    "video/mp4",
                    Some("broken.mp4".into()),
                )),
                serde_json::json!({}),
            )
            .await
            .expect_err("garbage must be rejected");
        match err {
            PluginError::NonRetryable(m) => {
                assert!(m.contains("AAC-LC"), "message should list codecs: {m}")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_non_media_and_wrong_input_kinds() {
        let plugin = VideoAudioExtractorPlugin::new();
        let err = plugin
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(b"%PDF-1.4".to_vec(), "application/pdf", None)),
                serde_json::json!({}),
            )
            .await
            .expect_err("pdf must be rejected");
        assert!(matches!(err, PluginError::InvalidInput(_)));

        let err = plugin
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                serde_json::json!({}),
            )
            .await
            .expect_err("documents must be rejected");
        assert!(matches!(err, PluginError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn cancellation_is_observed() {
        let ctx = ActivityContext::noop();
        ctx.cancellation_flag().store(true, Ordering::Relaxed);
        let err = VideoAudioExtractorPlugin::new()
            .execute(
                &ctx,
                PluginInput::Bytes(Blob::new(
                    wav(440.0, 0.2, 44_100, 1),
                    "audio/wav",
                    Some("c.wav".into()),
                )),
                serde_json::json!({}),
            )
            .await
            .expect_err("cancelled");
        assert!(matches!(err, PluginError::Cancelled));
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let plugin = VideoAudioExtractorPlugin::new();
        for cfg in [
            serde_json::json!({"sample_rate": 10}),
            serde_json::json!({"max_duration_secs": 0}),
            serde_json::json!({"nope": true}),
        ] {
            let err = plugin
                .execute(
                    &ActivityContext::noop(),
                    PluginInput::Bytes(Blob::new(wav(440.0, 0.05, 16_000, 1), "audio/wav", None)),
                    cfg.clone(),
                )
                .await
                .expect_err("config must be rejected");
            assert!(matches!(err, PluginError::InvalidConfig(_)), "config {cfg}");
        }
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = VideoAudioExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Bytes]);
        assert_eq!(m.produces, OutputKind::Bytes);
        let props = m.config_schema["properties"]
            .as_object()
            .expect("properties");
        for key in ["sample_rate", "mono", "max_duration_secs", "track"] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
        assert_eq!(
            m.config_schema["additionalProperties"],
            serde_json::json!(false)
        );
    }
}
