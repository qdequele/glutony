//! Sparse sheets: a few cells far apart must not cost a dense rows x cols grid.
//!
//! This binary installs a counting global allocator so it can assert on the peak
//! memory an extraction needs.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use meili_ingest_plugin_sdk::prelude::*;
use meili_ingest_plugin_xlsx::XlsxExtractorPlugin;
use rust_xlsxwriter::Workbook;

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const XLSX_MIME: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

fn input(bytes: Vec<u8>) -> PluginInput {
    PluginInput::Bytes(Blob::new(bytes, XLSX_MIME, Some("sparse.xlsx".into())))
}

/// Tests in this binary share the allocator counters; run them one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[tokio::test]
async fn a_stray_cell_on_the_last_row_does_not_allocate_the_full_grid() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    for col in 0..30u16 {
        ws.write_string(0, col, format!("h{col}")).unwrap();
    }
    ws.write_string(1, 0, "first").unwrap();
    ws.write_string(1_048_575, 29, "stray").unwrap();
    let bytes = wb.save_to_buffer().unwrap();

    let before = CURRENT.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let out = XlsxExtractorPlugin::new()
        .execute(
            &ActivityContext::noop(),
            input(bytes),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);

    let docs = out.into_documents().unwrap();
    assert_eq!(docs.len(), 2, "{docs:#?}");
    assert_eq!(docs[0].fields["h0"], "first");
    assert_eq!(docs[1].fields["h29"], "stray");
    assert_eq!(docs[1].fields["_row"], 1_048_576);
    // A dense 30 x 1,048,576 grid is about 1 GB; the sparse read needs a few MB.
    assert!(
        peak < 64 * 1024 * 1024,
        "extraction peaked at {peak} bytes above baseline"
    );
}

#[tokio::test]
async fn opposite_corners_of_a_sheet_are_read_without_a_dense_grid() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    ws.write_string(0, 0, "a").unwrap();
    ws.write_string(1_048_575, 16_383, "b").unwrap();
    let bytes = wb.save_to_buffer().unwrap();

    for (config, expected) in [
        (serde_json::json!({"has_headers": false}), 2),
        (serde_json::json!({"mode": "sheet"}), 1),
    ] {
        let out = XlsxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(bytes.clone()),
                config.clone(),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), expected, "{config}: {docs:#?}");
    }

    let out = XlsxExtractorPlugin::new()
        .execute(
            &ActivityContext::noop(),
            input(bytes),
            serde_json::json!({"has_headers": false}),
        )
        .await
        .unwrap();
    let docs = out.into_documents().unwrap();
    assert_eq!(docs[0].fields["col_1"], "a");
    assert_eq!(docs[1].fields["col_16384"], "b");
    assert_eq!(docs[1].fields["_row"], 1_048_576);
}
