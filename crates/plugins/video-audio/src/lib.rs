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
//!
//! # Memory
//!
//! Decoding, downmixing, resampling and encoding run as one streaming pass, a
//! packet at a time, so memory follows the input file plus the WAV being written,
//! not the decoded source PCM. A two-hour 48 kHz stereo track is never held as
//! gigabytes of f32 samples.
//!
//! The output still grows with the audio's length, so it is bounded too: with
//! `max_duration_secs` unset (or `null`), audio longer than
//! [`DEFAULT_MAX_DURATION_SECS`] (four hours) fails the step with a non-retryable
//! error instead of being truncated. An explicit `max_duration_secs` keeps the first
//! that many seconds, and may be larger than the default.
//!
//! # Usage reporting
//!
//! The step records no usage units. The decode is local and costs no third-party
//! money, and `whisper_transcriber` downstream reports the `audio_seconds` it was
//! billed for: reporting the decoded length here too would count every video's audio
//! twice in the usage aggregates, which sum `audio_seconds` over all steps. The
//! decoded length is still logged with each extraction.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use meili_ingest_plugin_sdk::prelude::*;
use rubato::audioadapter_buffers::owned::InterleavedOwned;
use rubato::{Fft, FixedSync, Indexing, Resampler};
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

/// Longest audio decoded when `max_duration_secs` is not set: four hours. Longer
/// audio fails the step rather than being truncated, so a decompression bomb or an
/// unexpectedly long recording cannot grow the output without bound.
pub const DEFAULT_MAX_DURATION_SECS: u64 = 4 * 3_600;

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
    /// Stop decoding after this many seconds of audio. Unset (or `null`) means
    /// [`DEFAULT_MAX_DURATION_SECS`], and audio longer than that fails the step.
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

    /// How much audio this step decodes.
    fn duration_limit(&self) -> DurationLimit {
        match self.max_duration_secs {
            Some(secs) => DurationLimit::Truncate(secs),
            None => DurationLimit::Reject(DEFAULT_MAX_DURATION_SECS),
        }
    }
}

/// What happens to audio past a duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurationLimit {
    /// An explicit `max_duration_secs`: keep the first this many seconds.
    Truncate(u64),
    /// The default ceiling: audio longer than this many seconds fails the step.
    Reject(u64),
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

/// What the blocking extraction returns to the async side.
#[derive(Debug)]
struct Extracted {
    /// The encoded WAV file.
    wav: Vec<u8>,
    /// Source channel count.
    channels: usize,
    /// Source sample rate.
    rate: u32,
    /// Source frames that went into the output (after `max_duration_secs`).
    source_frames: usize,
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
                        "description": "Keep only the first this many seconds. When unset (or null), audio longer than 14400 seconds (4 hours) fails the step."
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
        let origin = blob.origin.clone();
        let extension = extension(blob.filename.as_deref());
        ctx.heartbeat(format!("decoding {} bytes of {mime}", blob.data.len()));

        // Demuxing, decoding and resampling are CPU-bound and can run for minutes on a
        // long recording, so they happen on a blocking thread. The closure cannot touch
        // `ctx`, so it polls the shared cancellation flag instead.
        let cancelled = ctx.cancellation_flag();
        let data = blob.data;
        let mime_for_hint = mime.clone();
        let cfg_for_decode = cfg.clone();
        let limit = cfg.duration_limit();
        let extracted = run_blocking(move || {
            extract(
                data,
                &mime_for_hint,
                extension.as_deref(),
                &cfg_for_decode,
                limit,
                &cancelled,
            )
        })
        .await??;

        if extracted.skipped > 0 {
            tracing::warn!(
                plugin = NAME,
                skipped_packets = extracted.skipped,
                "skipped corrupt packets while decoding"
            );
        }
        ctx.check_cancelled()?;

        // No usage is recorded: decoding is local and costs no third-party money, and
        // the transcription step downstream reports the seconds it was billed for.
        // Reporting them here too would count every video's audio twice.
        let decoded_seconds = extracted.source_frames as f64 / f64::from(extracted.rate.max(1));

        let target_channels = if cfg.mono { 1 } else { extracted.channels };
        let sample_rate = cfg.sample_rate;
        let wav = extracted.wav;
        ctx.heartbeat(format!("encoded {} bytes of wav", wav.len()));
        tracing::info!(
            plugin = NAME,
            source_seconds = decoded_seconds,
            source_rate = extracted.rate,
            source_channels = extracted.channels,
            target_rate = sample_rate,
            target_channels,
            wav_bytes = wav.len(),
            "extracted audio"
        );
        // The extracted audio keeps the video's origin, so the transcript's ids are
        // tagged like the video's would be.
        Ok(PluginOutput::Bytes(
            Blob::new(wav, "audio/wav", Some(format!("{stem}.wav"))).with_origin(origin),
        ))
    }
}

/// Demux, decode, downmix, resample and WAV-encode in one streaming pass.
///
/// Each decoded packet goes straight through the downmix and the resampler into the
/// WAV writer, so memory follows the encoded output and the input file, not the
/// decoded source PCM: a long 48 kHz stereo track is never held in full as f32,
/// let alone in several copies.
fn extract(
    data: Vec<u8>,
    mime: &str,
    extension: Option<&str>,
    cfg: &VideoAudioConfig,
    limit: DurationLimit,
    cancelled: &Arc<AtomicBool>,
) -> Result<Extracted, PluginError> {
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

    let mut wav = Cursor::new(Vec::new());
    // The sink is built on the first decoded packet, once the source layout is known.
    let mut wav_slot = Some(&mut wav);
    let mut sink: Option<Sink<'_>> = None;
    let mut interleaved: Vec<f32> = Vec::new();
    let mut channels = 0usize;
    let mut rate = 0u32;
    let mut skipped = 0usize;
    let mut packets = 0usize;
    // Every sample decoded, and the ones kept once `max_duration_secs` applies.
    let mut decoded_samples = 0usize;
    let mut kept_samples = 0usize;
    let mut keep_limit = usize::MAX;
    let truncate_secs = match limit {
        DurationLimit::Truncate(secs) => Some(secs),
        DurationLimit::Reject(_) => None,
    };
    let max_frames = truncate_secs
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
                decoded_samples += interleaved.len();
                if let DurationLimit::Reject(secs) = limit
                    && channels > 0
                    && (decoded_samples / channels) as u64 > secs.saturating_mul(u64::from(rate))
                {
                    return Err(PluginError::non_retryable(format!(
                        "the audio is longer than max_duration_secs ({secs} s, the default); \
                         set max_duration_secs to a larger value to extract all of it, or \
                         split the file"
                    )));
                }
                if channels > 0
                    && rate > 0
                    && let Some(out) = wav_slot.take()
                {
                    // Honour max_duration exactly: the loop stops at a packet boundary,
                    // so only the samples within the limit are passed on.
                    if let Some(max) = truncate_secs {
                        keep_limit =
                            (max.saturating_mul(u64::from(rate)) as usize).saturating_mul(channels);
                    }
                    sink = Some(Sink::new(out, channels, rate, cfg)?);
                }
                if let Some(sink) = sink.as_mut() {
                    let take = interleaved.len().min(keep_limit - kept_samples);
                    sink.push(&interleaved[..take])?;
                    kept_samples += take;
                }
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
            && (decoded_samples / channels) as u64 >= max
        {
            break;
        }
    }

    let Some(sink) = sink.filter(|_| decoded_samples > 0) else {
        return Err(PluginError::non_retryable(format!(
            "decoded no audio from this file ({skipped} packets were corrupt); \
             {NAME} supports {SUPPORTED}"
        )));
    };
    sink.finish()?;

    Ok(Extracted {
        wav: wav.into_inner(),
        channels,
        rate,
        source_frames: kept_samples / channels,
        skipped,
    })
}

/// Copy any sample format out of a decoded buffer as interleaved f32.
fn append_interleaved(buf: &GenericAudioBufferRef<'_>, out: &mut Vec<f32>) {
    out.clear();
    buf.copy_to_vec_interleaved(out);
}

/// Takes decoded interleaved samples a packet at a time and writes the output WAV:
/// optional downmix to mono, resampling, then 16-bit encoding.
struct Sink<'w> {
    source_channels: usize,
    mono: bool,
    /// `None` when the source rate already is the target rate.
    resampler: Option<StreamResampler>,
    /// Samples of an incomplete frame, kept until the next packet completes it.
    carry: Vec<f32>,
    /// Scratch for one packet's worth of downmixed or converted samples.
    frames: Vec<f32>,
    scratch: Vec<f64>,
    writer: hound::WavWriter<&'w mut Cursor<Vec<u8>>>,
}

impl<'w> Sink<'w> {
    fn new(
        out: &'w mut Cursor<Vec<u8>>,
        source_channels: usize,
        source_rate: u32,
        cfg: &VideoAudioConfig,
    ) -> Result<Self, PluginError> {
        let out_channels = if cfg.mono { 1 } else { source_channels };
        let resampler = if source_rate == cfg.sample_rate {
            None
        } else {
            Some(StreamResampler::new(
                out_channels,
                source_rate,
                cfg.sample_rate,
            )?)
        };
        let spec = hound::WavSpec {
            channels: out_channels as u16,
            sample_rate: cfg.sample_rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let writer = hound::WavWriter::new(out, spec)
            .map_err(|e| PluginError::non_retryable(format!("cannot start wav encoding: {e}")))?;
        Ok(Self {
            source_channels,
            mono: cfg.mono,
            resampler,
            carry: Vec::new(),
            frames: Vec::new(),
            scratch: Vec::new(),
            writer,
        })
    }

    /// Feed interleaved samples in the source layout.
    fn push(&mut self, samples: &[f32]) -> Result<(), PluginError> {
        if samples.is_empty() {
            return Ok(());
        }
        let channels = self.source_channels;
        let downmix = self.mono && channels > 1;
        if !downmix && self.resampler.is_none() {
            // Nothing to do per frame: encode the samples as they are.
            return write_samples(&mut self.writer, samples.iter().copied());
        }

        // Downmixing and resampling work on whole frames, and a packet need not end
        // on one, so an incomplete frame waits for the next packet.
        let mut frames = std::mem::take(&mut self.frames);
        frames.clear();
        if channels > 1 {
            self.carry.extend_from_slice(samples);
            let whole = self.carry.len() / channels * channels;
            if downmix {
                for frame in self.carry[..whole].chunks_exact(channels) {
                    let sum: f32 = frame.iter().sum();
                    frames.push(sum / channels as f32);
                }
            } else {
                frames.extend_from_slice(&self.carry[..whole]);
            }
            self.carry.drain(..whole);
        } else {
            frames.extend_from_slice(samples);
        }

        let result = match self.resampler.as_mut() {
            None => write_samples(&mut self.writer, frames.iter().copied()),
            Some(resampler) => {
                self.scratch.clear();
                self.scratch.extend(frames.iter().map(|&s| f64::from(s)));
                let writer = &mut self.writer;
                resampler.push(&self.scratch, &mut |out| {
                    write_samples(writer, out.iter().map(|&v| v as f32))
                })
            }
        };
        self.frames = frames;
        result
    }

    /// Flush the resampler and finalize the WAV header.
    fn finish(mut self) -> Result<(), PluginError> {
        if let Some(resampler) = self.resampler.take() {
            let writer = &mut self.writer;
            resampler.finish(&mut |out| write_samples(writer, out.iter().map(|&v| v as f32)))?;
        }
        self.writer
            .finalize()
            .map_err(|e| PluginError::non_retryable(format!("cannot finalize wav: {e}")))
    }
}

/// Encode f32 samples in [-1, 1] as 16-bit PCM.
fn write_samples<W: std::io::Write + std::io::Seek>(
    writer: &mut hound::WavWriter<W>,
    samples: impl Iterator<Item = f32>,
) -> Result<(), PluginError> {
    for s in samples {
        let clamped = (s.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
        writer
            .write_sample(clamped)
            .map_err(|e| PluginError::non_retryable(format!("cannot write wav samples: {e}")))?;
    }
    Ok(())
}

/// Band-limited resampling with rubato's FFT resampler, fed a packet at a time.
///
/// This reproduces `Resampler::process_all_into_buffer` exactly (same chunking, the
/// same trim of the startup delay, the same final partial chunk and zero padding, the
/// same output length) without needing the whole clip in memory: input is buffered
/// only until a chunk is full, and output is handed on as soon as no later step can
/// rewrite it.
struct StreamResampler {
    resampler: Fft<f64>,
    channels: usize,
    /// Interleaved input not yet fed to the resampler.
    pending: Vec<f64>,
    /// Reused input and output chunk buffers.
    chunk_in: Vec<f64>,
    chunk_out: Vec<f64>,
    /// Total input frames received.
    frames_in: usize,
    /// Output frames from logical position `flushed` on (interleaved).
    out: Vec<f64>,
    flushed: usize,
    /// Logical write position and output length, as in `process_all_into_buffer`.
    output_offset: usize,
    output_len: usize,
    frames_to_trim: usize,
}

impl StreamResampler {
    fn new(channels: usize, from_rate: u32, to_rate: u32) -> Result<Self, PluginError> {
        let resampler = Fft::<f64>::new(
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
        let frames_to_trim = resampler.output_delay();
        let chunk_in = vec![0.0; resampler.input_frames_max() * channels];
        let chunk_out = vec![0.0; resampler.output_frames_max() * channels];
        Ok(Self {
            resampler,
            channels,
            pending: Vec::new(),
            chunk_in,
            chunk_out,
            frames_in: 0,
            out: Vec::new(),
            flushed: 0,
            output_offset: 0,
            output_len: 0,
            frames_to_trim,
        })
    }

    /// Feed whole interleaved frames; `emit` receives finished output frames.
    fn push(
        &mut self,
        frames: &[f64],
        emit: &mut dyn FnMut(&[f64]) -> Result<(), PluginError>,
    ) -> Result<(), PluginError> {
        self.pending.extend_from_slice(frames);
        self.frames_in += frames.len() / self.channels;
        // Like `process_all_into_buffer`, process full chunks while more than one
        // chunk is left: the last one (even if full) is handled as the partial chunk.
        while self.pending.len() / self.channels > self.resampler.input_frames_next() {
            let consumed = self.process(None)?;
            self.pending.drain(..consumed * self.channels);
            let t = self.frames_to_trim;
            if t > 0 && self.output_len > t {
                // `process_all_into_buffer` moves `t` frames from `t` to the start of
                // its buffer (a zero-initialised one, hence the resize).
                if self.out.len() < 2 * t * self.channels {
                    self.out.resize(2 * t * self.channels, 0.0);
                }
                self.out
                    .copy_within(t * self.channels..2 * t * self.channels, 0);
                self.output_len -= t;
                self.output_offset -= t;
                self.frames_to_trim = 0;
            }
            if self.frames_to_trim == 0 {
                // Everything before the write position is final.
                let done = (self.output_offset - self.flushed) * self.channels;
                emit(&self.out[..done])?;
                self.out.drain(..done);
                self.flushed = self.output_offset;
            }
        }
        Ok(())
    }

    /// Process the last partial chunk, pad with silence to the expected length and
    /// emit the rest of the output.
    fn finish(
        mut self,
        emit: &mut dyn FnMut(&[f64]) -> Result<(), PluginError>,
    ) -> Result<(), PluginError> {
        let expected = (self.resampler.resample_ratio() * self.frames_in as f64).ceil() as usize;
        let left = self.pending.len() / self.channels;
        if left > 0 {
            self.process(Some(left))?;
        }
        while self.output_len < expected {
            self.process(Some(0))?;
        }
        if expected < self.flushed {
            return Err(PluginError::non_retryable(
                "resampling failed: emitted more output than the clip length",
            ));
        }
        emit(&self.out[..(expected - self.flushed) * self.channels])
    }

    /// Run one chunk and write its output at the current write position. Returns the
    /// number of input frames consumed.
    fn process(&mut self, partial_len: Option<usize>) -> Result<usize, PluginError> {
        let ch = self.channels;
        let available = (self.pending.len() / ch).min(self.chunk_in.len() / ch);
        let n = partial_len.unwrap_or(available).min(available);
        self.chunk_in[..n * ch].copy_from_slice(&self.pending[..n * ch]);

        let in_frames = self.chunk_in.len() / ch;
        let out_frames = self.chunk_out.len() / ch;
        let input = InterleavedOwned::new_from(std::mem::take(&mut self.chunk_in), ch, in_frames)
            .map_err(|e| {
            PluginError::non_retryable(format!("cannot wrap decoded audio: {e}"))
        })?;
        let mut output =
            InterleavedOwned::new_from(std::mem::take(&mut self.chunk_out), ch, out_frames)
                .map_err(|e| {
                    PluginError::non_retryable(format!("cannot wrap resampled audio: {e}"))
                })?;
        let indexing = Indexing {
            input_offset: 0,
            output_offset: 0,
            active_channels_mask: None,
            partial_len,
        };
        let result = self
            .resampler
            .process_into_buffer(&input, &mut output, Some(&indexing));
        self.chunk_in = input.take_data();
        self.chunk_out = output.take_data();
        let (consumed, produced) =
            result.map_err(|e| PluginError::non_retryable(format!("resampling failed: {e}")))?;

        let at = (self.output_offset - self.flushed) * ch;
        let end = at + produced * ch;
        if self.out.len() < end {
            self.out.resize(end, 0.0);
        }
        self.out[at..end].copy_from_slice(&self.chunk_out[..produced * ch]);
        self.output_offset += produced;
        self.output_len += produced;
        Ok(consumed)
    }
}

/// Filename without its extension, folders kept, sanitized for use in an id.
fn file_stem(filename: Option<&str>) -> String {
    meili_ingest_plugin_sdk::filename_id_stem(filename)
        .unwrap_or_else(|| meili_ingest_plugin_sdk::sanitize_id("audio"))
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

    /// `secs` seconds of silence as 1 kHz mono 16-bit WAV, written directly so a
    /// four-hour clip stays cheap to build (29 MB). At 1 kHz with `sample_rate: 1000`
    /// the extractor passes the samples through without resampling.
    fn long_silence_wav(secs: u64) -> Vec<u8> {
        let data_len = (secs * 1_000 * 2) as u32;
        let mut out = Vec::with_capacity(44 + data_len as usize);
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data_len).to_le_bytes());
        out.extend_from_slice(b"WAVEfmt ");
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
        out.extend_from_slice(&1u16.to_le_bytes()); // mono
        out.extend_from_slice(&1_000u32.to_le_bytes());
        out.extend_from_slice(&2_000u32.to_le_bytes());
        out.extend_from_slice(&2u16.to_le_bytes());
        out.extend_from_slice(&16u16.to_le_bytes());
        out.extend_from_slice(b"data");
        out.extend_from_slice(&data_len.to_le_bytes());
        out.resize(44 + data_len as usize, 0);
        out
    }

    #[tokio::test]
    async fn audio_longer_than_the_default_ceiling_fails_instead_of_buffering() {
        let source = long_silence_wav(DEFAULT_MAX_DURATION_SECS + 1);
        let plugin = VideoAudioExtractorPlugin::new();
        // Unset and an explicit null both mean the default ceiling, not unlimited.
        for cfg in [
            serde_json::json!({"sample_rate": 1_000}),
            serde_json::json!({"sample_rate": 1_000, "max_duration_secs": null}),
        ] {
            let err = plugin
                .execute(
                    &ActivityContext::noop(),
                    PluginInput::Bytes(Blob::new(
                        source.clone(),
                        "audio/wav",
                        Some("long.wav".into()),
                    )),
                    cfg.clone(),
                )
                .await
                .expect_err("over the default ceiling");
            let PluginError::NonRetryable(msg) = &err else {
                panic!("expected NonRetryable for {cfg}, got {err:?}");
            };
            assert!(msg.contains("max_duration_secs"), "{msg}");
            assert!(msg.contains("14400"), "{msg}");
        }

        // An explicit larger value is honoured: the whole clip comes out.
        let out = run(
            source,
            "audio/wav",
            "long.wav",
            serde_json::json!({"sample_rate": 1_000, "max_duration_secs": DEFAULT_MAX_DURATION_SECS + 10}),
        )
        .await;
        let PluginOutput::Bytes(blob) = out else {
            panic!("expected bytes");
        };
        let frames = (blob.data.len() - 44) / 2;
        assert_eq!(frames as u64, (DEFAULT_MAX_DURATION_SECS + 1) * 1_000);
    }

    #[test]
    fn the_ceiling_rejects_only_audio_past_it() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cfg = VideoAudioConfig::default();
        let input = wav_frames(2 * 44_100 + 1, 44_100, 2);
        let err = extract(
            input,
            "audio/wav",
            Some("wav"),
            &cfg,
            DurationLimit::Reject(2),
            &cancelled,
        )
        .expect_err("one frame past the ceiling");
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err:?}");
        // Exactly at the ceiling is fine, and the output is not truncated.
        let input = wav_frames(2 * 44_100, 44_100, 2);
        let expected = reference::extract(input.clone(), &cfg);
        let got = extract(
            input,
            "audio/wav",
            Some("wav"),
            &cfg,
            DurationLimit::Reject(2),
            &cancelled,
        )
        .expect("at the ceiling");
        assert!(got.wav == expected);
    }

    #[test]
    fn unset_means_the_default_ceiling_and_a_value_truncates() {
        assert_eq!(
            VideoAudioConfig::parse(serde_json::Value::Null)
                .expect("config")
                .duration_limit(),
            DurationLimit::Reject(DEFAULT_MAX_DURATION_SECS)
        );
        assert_eq!(
            VideoAudioConfig::parse(serde_json::json!({"max_duration_secs": null}))
                .expect("config")
                .duration_limit(),
            DurationLimit::Reject(DEFAULT_MAX_DURATION_SECS)
        );
        assert_eq!(
            VideoAudioConfig::parse(serde_json::json!({"max_duration_secs": 30}))
                .expect("config")
                .duration_limit(),
            DurationLimit::Truncate(30)
        );
        assert_eq!(DEFAULT_MAX_DURATION_SECS, 4 * 3_600);
    }

    #[tokio::test]
    async fn decoding_records_no_usage() {
        // The transcription step downstream reports the seconds it was billed for;
        // reporting them here too would count every video's audio twice.
        let ctx = ActivityContext::noop();
        VideoAudioExtractorPlugin::new()
            .execute(
                &ctx,
                PluginInput::Bytes(Blob::new(
                    wav(440.0, 0.75, 44_100, 2),
                    "audio/wav",
                    Some("meeting.wav".into()),
                )),
                serde_json::json!({}),
            )
            .await
            .expect("extraction succeeds");

        let usage = ctx.usage();
        assert!(usage.is_empty(), "recorded {usage:?}");
        assert_eq!(usage.audio_seconds, 0.0);
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

    /// The whole-clip pipeline this plugin used before it streamed: decode every
    /// packet into one buffer, truncate, downmix, resample with
    /// `process_all_into_buffer`, encode. The streaming pass must produce the same
    /// bytes.
    mod reference {
        use super::super::*;

        pub fn decode(data: Vec<u8>, cfg: &VideoAudioConfig) -> (Vec<f32>, usize, u32) {
            let mss = MediaSourceStream::new(Box::new(Cursor::new(data)), Default::default());
            let mut hint = Hint::new();
            hint.mime_type("audio/wav");
            let mut format = symphonia::default::get_probe()
                .probe(
                    &hint,
                    mss,
                    FormatOptions::default(),
                    MetadataOptions::default(),
                )
                .expect("probe");
            let track = format.default_track(TrackType::Audio).expect("track");
            let track_id = track.id;
            let Some(CodecParameters::Audio(params)) = track.codec_params.clone() else {
                panic!("no audio params");
            };
            let mut decoder = symphonia::default::get_codecs()
                .make_audio_decoder(&params, &AudioDecoderOptions::default())
                .expect("decoder");
            let mut samples = Vec::new();
            let mut interleaved = Vec::new();
            let (mut channels, mut rate) = (0usize, 0u32);
            let max_frames = cfg
                .max_duration_secs
                .map(|s| s.saturating_mul(u64::from(params.sample_rate.unwrap_or(48_000))));
            while let Ok(Some(packet)) = format.next_packet() {
                if packet.track_id != track_id {
                    continue;
                }
                let buf = decoder.decode(&packet).expect("decode");
                append_interleaved(&buf, &mut interleaved);
                if channels == 0 {
                    channels = buf.spec().channels().count();
                    rate = buf.spec().rate();
                }
                samples.append(&mut interleaved);
                if let Some(max) = max_frames
                    && (samples.len() / channels) as u64 >= max
                {
                    break;
                }
            }
            if let Some(max) = cfg.max_duration_secs {
                let keep = (max.saturating_mul(u64::from(rate)) as usize).saturating_mul(channels);
                samples.truncate(keep.min(samples.len()));
            }
            (samples, channels, rate)
        }

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

        fn resample(samples: Vec<f32>, channels: usize, from: u32, to: u32) -> Vec<f32> {
            if from == to || samples.is_empty() {
                return samples;
            }
            let frames = samples.len() / channels.max(1);
            let data: Vec<f64> = samples.into_iter().map(f64::from).collect();
            let input = InterleavedOwned::new_from(data, channels, frames).expect("wrap");
            let mut resampler = Fft::<f64>::new(
                from as usize,
                to as usize,
                RESAMPLE_CHUNK,
                channels,
                FixedSync::Both,
            )
            .expect("resampler");
            let needed = resampler.process_all_needed_output_len(frames);
            let mut output = InterleavedOwned::<f64>::new(0.0, channels, needed);
            let (_, produced) = resampler
                .process_all_into_buffer(&input, &mut output, frames, None)
                .expect("resample");
            let mut data = output.take_data();
            data.truncate(produced * channels);
            data.into_iter().map(|v| v as f32).collect()
        }

        fn encode(samples: &[f32], channels: u16, rate: u32) -> Vec<u8> {
            let spec = hound::WavSpec {
                channels,
                sample_rate: rate,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            };
            let mut cursor = Cursor::new(Vec::new());
            {
                let mut w = hound::WavWriter::new(&mut cursor, spec).expect("writer");
                for &s in samples {
                    let v = (s.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
                    w.write_sample(v).expect("sample");
                }
                w.finalize().expect("finalize");
            }
            cursor.into_inner()
        }

        pub fn extract(data: Vec<u8>, cfg: &VideoAudioConfig) -> Vec<u8> {
            let (samples, channels, rate) = decode(data, cfg);
            let target_channels = if cfg.mono { 1 } else { channels };
            let mixed = if cfg.mono {
                downmix_to_mono(&samples, channels)
            } else {
                samples
            };
            let out = resample(mixed, target_channels, rate, cfg.sample_rate);
            encode(&out, target_channels as u16, cfg.sample_rate)
        }
    }

    /// A WAV of exactly `frames` frames; each channel carries a different tone.
    fn wav_frames(frames: usize, rate: u32, channels: u16) -> Vec<u8> {
        let spec = hound::WavSpec {
            channels,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut w = hound::WavWriter::new(&mut cursor, spec).expect("writer");
            for i in 0..frames {
                let t = i as f64 / f64::from(rate);
                for c in 0..channels {
                    let f = 300.0 + 170.0 * f64::from(c) + 40.0 * t;
                    let v = 0.7 * (2.0 * std::f64::consts::PI * f * t).sin();
                    w.write_sample((v * 32767.0) as i16).expect("sample");
                }
            }
            w.finalize().expect("finalize");
        }
        cursor.into_inner()
    }

    fn streamed(data: Vec<u8>, cfg: &VideoAudioConfig) -> Vec<u8> {
        extract(
            data,
            "audio/wav",
            Some("wav"),
            cfg,
            cfg.duration_limit(),
            &Arc::new(AtomicBool::new(false)),
        )
        .expect("extraction succeeds")
        .wav
    }

    #[test]
    fn streaming_matches_the_whole_clip_pipeline_byte_for_byte() {
        // Lengths around the resampler's chunk boundaries, clips shorter than the
        // startup delay, and a few seconds spanning hundreds of chunks.
        let lengths = [1, 7, 300, 1023, 1024, 1025, 2047, 2048, 2049, 4097, 130_001];
        for (rate, channels, cfg) in [
            (44_100, 2, serde_json::json!({})),
            (48_000, 1, serde_json::json!({})),
            (22_050, 2, serde_json::json!({"mono": false})),
            (8_000, 2, serde_json::json!({})),
            (16_000, 2, serde_json::json!({})),
            (16_000, 2, serde_json::json!({"mono": false})),
            (44_100, 1, serde_json::json!({"sample_rate": 48_000})),
            (44_100, 2, serde_json::json!({"max_duration_secs": 1})),
            (
                32_000,
                3,
                serde_json::json!({"mono": false, "sample_rate": 8_000}),
            ),
        ] {
            let cfg = VideoAudioConfig::parse(cfg).expect("config");
            for &frames in &lengths {
                let input = wav_frames(frames, rate, channels);
                let expected = reference::extract(input.clone(), &cfg);
                let got = streamed(input, &cfg);
                assert!(
                    got == expected,
                    "{frames} frames of {rate} Hz x{channels} with {cfg:?}: \
                     {} bytes streamed, {} bytes expected",
                    got.len(),
                    expected.len()
                );
            }
        }
    }

    #[test]
    fn a_packet_need_not_end_on_a_frame() {
        for cfg in [
            serde_json::json!({}),
            serde_json::json!({"mono": false}),
            serde_json::json!({"mono": false, "sample_rate": 44_100}),
        ] {
            let cfg = VideoAudioConfig::parse(cfg).expect("config");
            let input = wav_frames(9_001, 44_100, 2);
            let expected = reference::extract(input.clone(), &cfg);
            let (samples, channels, rate) = reference::decode(input, &cfg);

            let mut out = Cursor::new(Vec::new());
            {
                let mut sink = Sink::new(&mut out, channels, rate, &cfg).expect("sink");
                // Odd-sized pieces split frames across pushes.
                for piece in samples.chunks(777) {
                    sink.push(piece).expect("push");
                }
                sink.finish().expect("finish");
            }
            assert!(
                out.into_inner() == expected,
                "split packets changed the output with {cfg:?}"
            );
        }
    }
}
