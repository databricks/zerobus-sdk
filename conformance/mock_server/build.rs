use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Canonical Zerobus data-plane proto (single source of truth, shared with the SDKs).
    let zerobus_proto_dir = manifest_dir.join("../../rust/sdk");
    let zerobus_proto = zerobus_proto_dir.join("zerobus_service.proto");
    // Out-of-band control plane proto owned by this crate.
    let mock_proto_dir = manifest_dir.join("proto");
    let mock_proto = mock_proto_dir.join("mock_admin.proto");

    println!("cargo:rerun-if-changed={}", zerobus_proto.display());
    println!("cargo:rerun-if-changed={}", mock_proto.display());

    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    let mut includes = vec![zerobus_proto_dir.clone(), mock_proto_dir.clone()];
    // Well-known types (google/protobuf/duration.proto) come from the vendored protoc.
    includes.push(protoc_bin_vendored::include_path()?);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[zerobus_proto, mock_proto], &includes)?;

    Ok(())
}
