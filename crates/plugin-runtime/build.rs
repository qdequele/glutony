//! Compiles `proto/plugin.proto` into the tonic client/server used by the `grpc` feature.
//! Requires `protoc` on `PATH` (or the `PROTOC` env var) when the feature is enabled.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        let proto = "../../proto/plugin.proto";
        println!("cargo:rerun-if-changed={proto}");
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .compile_protos(&[proto], &["../../proto"])?;
    }
    Ok(())
}
