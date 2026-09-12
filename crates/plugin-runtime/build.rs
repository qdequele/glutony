//! Compiles `proto/plugin.proto` into the tonic client/server used by the `grpc`
//! feature.
//!
//! Uses `protox`, a pure-Rust protobuf compiler, rather than shelling out to a
//! `protoc` binary. That keeps the build self-contained: no system package to
//! install, nothing to go missing in a slim container image, and no dependency on a
//! distribution shipping the well-known types alongside the compiler.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        use prost::Message;

        let proto = "../../proto/plugin.proto";
        let include = "../../proto";
        println!("cargo:rerun-if-changed={proto}");

        // Parse and resolve the descriptor set ourselves, then hand it to
        // tonic-prost-build with protoc disabled.
        let descriptors = protox::compile([proto], [include])?;
        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
        let descriptor_path = out_dir.join("plugin_descriptor.bin");
        std::fs::write(&descriptor_path, descriptors.encode_to_vec())?;

        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .file_descriptor_set_path(&descriptor_path)
            .skip_protoc_run()
            .compile_protos(&[proto], &[include])?;
    }
    Ok(())
}
