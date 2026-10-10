//! `sqlx::migrate!` embeds `migrations/` at compile time, but on stable Rust a proc
//! macro cannot ask Cargo to watch a directory: without this, a new migration file is
//! not embedded until something else in the crate changes.

fn main() {
    println!("cargo:rerun-if-changed=../../migrations");
}
