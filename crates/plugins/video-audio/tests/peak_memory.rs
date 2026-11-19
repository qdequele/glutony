//! Peak heap use of one extraction. This file is its own test binary with a single
//! test, so the counting allocator below sees no allocations from other tests.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};

use meili_ingest_plugin_sdk::prelude::*;
use meili_ingest_plugin_video_audio::VideoAudioExtractorPlugin;

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    let now = CURRENT.fetch_add(by, Ordering::SeqCst) + by;
    PEAK.fetch_max(now, Ordering::SeqCst);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::SeqCst);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            if new_size >= layout.size() {
                grew(new_size - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::SeqCst);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn stereo_wav(secs: usize, rate: u32) -> Vec<u8> {
    let spec = hound::WavSpec {
        channels: 2,
        sample_rate: rate,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut w = hound::WavWriter::new(&mut cursor, spec).expect("writer");
        for i in 0..secs * rate as usize {
            let t = i as f64 / f64::from(rate);
            let v = (0.5 * (2.0 * std::f64::consts::PI * 440.0 * t).sin() * 32767.0) as i16;
            w.write_sample(v).expect("sample");
            w.write_sample(v).expect("sample");
        }
        w.finalize().expect("finalize");
    }
    cursor.into_inner()
}

/// Memory must follow the 16 kHz mono output, not the decoded source PCM: a long
/// 48 kHz stereo track used to be held as f32, then as a mono copy, then as f64,
/// several times the size of the WAV that comes out.
#[tokio::test(flavor = "multi_thread")]
async fn peak_memory_follows_the_output_not_the_decoded_source() {
    let secs = 60;
    let input = stereo_wav(secs, 48_000);
    let input_len = input.len();
    let wav_len = secs * 16_000 * 2 + 44;
    let blob = Blob::new(input, "audio/wav", Some("long.wav".to_string()));
    let ctx = ActivityContext::noop();

    let baseline = CURRENT.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
    let out = VideoAudioExtractorPlugin::new()
        .execute(&ctx, PluginInput::Bytes(blob), serde_json::json!({}))
        .await
        .expect("extraction succeeds");
    let peak = PEAK.load(Ordering::SeqCst) - baseline;

    match &out {
        PluginOutput::Bytes(b) => assert_eq!(b.data.len(), wav_len),
        other => panic!("expected bytes, got {other:?}"),
    }
    // The input is already counted in the baseline. Allow the growing WAV buffer
    // (a Vec doubling can briefly hold old and new capacity) plus small scratch.
    let budget = 4 * wav_len + (2 << 20);
    assert!(
        peak <= budget,
        "peak heap growth {peak} bytes for a {input_len}-byte input and a {wav_len}-byte \
         output; budget {budget}"
    );
}
