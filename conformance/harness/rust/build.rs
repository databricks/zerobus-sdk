fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the vendored protoc so the build does not depend on a system install.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    println!("cargo:rerun-if-changed=../proto/test_harness.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../proto/test_harness.proto"], &["../proto"])?;

    Ok(())
}
