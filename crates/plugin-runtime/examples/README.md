# Writing external plugins

The worker loads two kinds of external plugins through this crate:

| kind | how to ship | `EXTERNAL_PLUGINS` entry |
|---|---|---|
| WASM module | a `.wasm` file built with [extism-pdk](https://github.com/extism/rust-pdk) | `wasm:/plugins/upper.wasm` |
| gRPC container | any language, implements `proto/plugin.proto` | `grpc:http://whisper:50051` |

Both speak the same JSON types as built-in plugins (`crates/plugin-sdk/src/types.rs`).

## A WASM plugin in Rust

Install the target and (optionally) the extism CLI for local testing:

```sh
rustup target add wasm32-wasip1
cargo install extism-cli   # optional: `extism call` lets you poke the module
```

Create a `cdylib` crate. It depends on the SDK with the `wasm` feature (which brings in
`extism-pdk` and a tiny `futures` executor) and on `extism-pdk` itself for the
`#[plugin_fn]` export macro:

```toml
# Cargo.toml
[package]
name = "upper-plugin"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
meili-ingest-plugin-sdk = { path = "../../crates/plugin-sdk", features = ["wasm"] }
extism-pdk = "1"
serde_json = "1"
```

Implement `Plugin` exactly as you would for a built-in, then export the two entry points
the runtime looks for (`manifest` is optional, `execute` is required):

```rust
// src/lib.rs
use extism_pdk::{plugin_fn, FnResult};
use meili_ingest_plugin_sdk::prelude::*;
use meili_ingest_plugin_sdk::wasm::{manifest_json, run_plugin};

struct Upper;

#[async_trait]
impl Plugin for Upper {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new("upper", env!("CARGO_PKG_VERSION"))
            .description("Uppercases document content")
            .accepts([InputKind::Documents, InputKind::Many])
            .produces(OutputKind::Documents)
    }

    async fn execute(
        &self,
        _ctx: &ActivityContext,
        input: PluginInput,
        _config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        let docs = input.into_documents()?;
        Ok(PluginOutput::Documents(
            docs.into_iter()
                .map(|mut d| { d.content = d.content.to_uppercase(); d })
                .collect(),
        ))
    }
}

#[plugin_fn]
pub fn manifest() -> FnResult<Vec<u8>> {
    Ok(manifest_json(&Upper))
}

#[plugin_fn]
pub fn execute(input: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(run_plugin(&Upper, &input))
}
```

`run_plugin` decodes the `{"input": PluginInput, "config": {...}}` envelope, runs
`execute` on a single-threaded executor (there is no tokio inside WASM) and encodes the
result as `{"ok": PluginOutput}` or `{"error": {"message": "...", "retryable": bool}}`.
It never panics on bad input: decoding failures become a non-retryable error reply.

Build and try it:

```sh
cargo build --release --target wasm32-wasip1
extism call target/wasm32-wasip1/release/upper_plugin.wasm manifest --wasi
extism call target/wasm32-wasip1/release/upper_plugin.wasm execute --wasi \
  --input '{"input":{"type":"documents","value":[{"id":"a","content":"hi"}]},"config":{}}'
# {"ok":{"type":"documents","value":[{"id":"a","content":"HI","meta":{}}]}}
```

Ship the `.wasm` file to the worker image (or a mounted volume) and register it:

```sh
EXTERNAL_PLUGINS="wasm:/plugins/upper_plugin.wasm"
```

Limits worth knowing: the guest runs with WASI enabled but has no network access unless
the host manifest allows hosts; heartbeats inside the guest are discarded (the host
heartbeats once per call); one call runs at a time per loaded module.

## A gRPC plugin in another language

Generate stubs from `proto/plugin.proto` and implement `GetManifest` + `Execute`. The
header comment of the proto file contains complete Python and Go skeletons, the JSON
shapes of every payload and the error/retry semantics. Run the container next to the
workers and register it:

```sh
EXTERNAL_PLUGINS="grpc:http://whisper:50051"
```

Several plugins: separate entries with commas.
