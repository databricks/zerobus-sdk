use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Only the MockAdmin control plane is generated here. The Zerobus data-plane types are
    // reused from the SDK (see src/lib.rs), so we do NOT compile zerobus_service.proto.
    let mock_proto_dir = manifest_dir.join("proto");
    let mock_proto = mock_proto_dir.join("mock_admin.proto");

    println!("cargo:rerun-if-changed={}", mock_proto.display());

    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    let includes = vec![mock_proto_dir.clone(), protoc_bin_vendored::include_path()?];

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[mock_proto], &includes)?;

    Ok(())
}
